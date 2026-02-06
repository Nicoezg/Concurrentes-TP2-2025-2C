//! Actor que representa un vehículo que solicita cargas de combustible.
//!
//! Gestiona la comunicación con el GPS y las estaciones de servicio,
//! implementando reintentos y timeouts para garantizar la confiabilidad.

use actix::{Actor, Addr, AsyncContext, Context, Handler, SpawnHandle};
use common::{
    ActorConnected, ActorType, Coordenadas, EnviarMensaje, ErrorConexion, MensajeRecibido,
    RespuestaCarga, RespuestaDireccionEstacion, SolicitarCarga, SolicitarDireccionEstacion,
};
use log::{error, info, warn};
use network::CommunicationHandler;
use std::time::Duration;
use tokio::net::TcpStream;

/// Tiempo de espera en segundos para recibir respuesta de la estación.
const TIEMPO_ESPERA_TIMEOUT: u64 = 30;
/// Número máximo de reintentos para conectar a una estación.
const REINTENTOS_MAXIMOS: u32 = 5;

/// Actor que representa un vehículo.
///
/// Mantiene el estado de conexión con el GPS, la dirección de la estación actual,
/// y gestiona el proceso de solicitud de carga con reintentos automáticos.
pub struct Vehiculo {
    /// ID único del vehículo
    id: u32,
    /// ID de la compañía a la que pertenece
    compania_id: u32,
    /// Coordenadas geográficas actuales
    coordenadas_actuales: Coordenadas,
    /// Handler de comunicación con el GPS
    handler_gps: Option<Addr<CommunicationHandler<Self>>>,
    /// Cantidad de combustible solicitada en la carga actual
    cantidad_a_cargar: Option<f64>,
    /// Contador de cargas realizadas (para idempotencia)
    contador_cargas: u32,
    /// Indica si está esperando respuesta de una carga
    esperando_respuesta_carga: bool,
    /// Dirección de la estación actualmente seleccionada
    direccion_estacion_actual: Option<String>,
    /// Handle del temporizador de timeout
    timeout_handle: Option<SpawnHandle>,
    /// Número de reintentos realizados en el intento actual
    reintentos_actuales: u32,
}

impl Vehiculo {
    /// Crea un nuevo vehículo con los parámetros especificados.
    pub fn new(id: u32, compania_id: u32, coordenadas: Coordenadas) -> Self {
        Self {
            id,
            compania_id,
            coordenadas_actuales: coordenadas,
            handler_gps: None,
            cantidad_a_cargar: None,
            contador_cargas: 0,
            esperando_respuesta_carga: false,
            direccion_estacion_actual: None,
            timeout_handle: None,
            reintentos_actuales: 0,
        }
    }

    /// Solicita la dirección de una estación cercana al GPS.
    fn solicitar_direccion(&self) {
        if let Some(handler) = &self.handler_gps {
            let solicitud = SolicitarDireccionEstacion {
                coordenadas_actuales: self.coordenadas_actuales,
            };

            match serde_json::to_string(&solicitud) {
                Ok(payload) => {
                    info!(
                        "Vehículo {}: Solicitando dirección de estación al GPS",
                        self.id
                    );
                    handler.do_send(EnviarMensaje {
                        tipo_mensaje: "SolicitarDireccionEstacion".to_string(),
                        payload,
                    });
                }
                Err(e) => error!("Vehículo {}: Error serializando solicitud: {}", self.id, e),
            }
        } else {
            warn!("Vehículo {}: No hay handler GPS configurado", self.id);
        }
    }

    /// Intenta conectar a la estación y solicitar carga con reintentos automáticos.
    ///
    /// Si falla, reintenta hasta alcanzar el límite de REINTENTOS_MAXIMOS.
    fn iniciar_intento_carga(&mut self, ctx: &mut Context<Self>) {
        if let Some(direccion) = &self.direccion_estacion_actual {
            let direccion = direccion.clone();

            if self.reintentos_actuales >= REINTENTOS_MAXIMOS {
                warn!(
                    "Vehículo {}: Se alcanzó el máximo de {} reintentos. Abortando.",
                    self.id, REINTENTOS_MAXIMOS
                );
                self.esperando_respuesta_carga = false;
                return;
            }

            self.esperando_respuesta_carga = true;

            if let Some(handle) = self.timeout_handle {
                ctx.cancel_future(handle);
            }

            self.timeout_handle = Some(ctx.run_later(
                Duration::from_secs(TIEMPO_ESPERA_TIMEOUT),
                move |act, ctx| {
                    if act.esperando_respuesta_carga {
                        act.reintentos_actuales += 1;
                        warn!(
                            "Vehículo {}: Timeout (reintento {}/{}) esperando respuesta. Reintentando...",
                            act.id,
                            act.reintentos_actuales,
                            REINTENTOS_MAXIMOS
                        );
                        act.iniciar_intento_carga(ctx);
                    }
                },
            ));

            let vehiculo_id = self.id;
            let compania_id = self.compania_id;
            let cantidad = self.cantidad_a_cargar.unwrap_or(0.0);
            let contador = self.contador_cargas;
            let addr_clone = ctx.address();
            let direccion_clonada = direccion.clone();

            actix_rt::spawn(async move {
                info!(
                    "Vehículo {}: Conectando a estación en {}…",
                    vehiculo_id, direccion_clonada
                );

                match TcpStream::connect(&direccion_clonada).await {
                    Ok(stream) => {
                        info!("Vehículo {}: Conectado a estación", vehiculo_id);

                        let handler_addr = CommunicationHandler::new_initiator(
                            stream,
                            direccion_clonada.clone(),
                            addr_clone.clone(),
                            ActorType::Vehiculo,
                            vehiculo_id,
                        )
                        .start();

                        tokio::time::sleep(Duration::from_millis(300)).await;

                        if let Ok(payload) = serde_json::to_string(&SolicitarCarga {
                            vehiculo_id,
                            compania_id,
                            cantidad,
                            contador_carga: contador,
                        }) {
                            info!(
                                "Vehículo {}: Enviando solicitud de carga de {} litros (contador: {})",
                                vehiculo_id, cantidad, contador
                            );

                            handler_addr.do_send(EnviarMensaje {
                                tipo_mensaje: "SolicitarCarga".to_string(),
                                payload,
                            });
                        }
                    }
                    Err(e) => {
                        error!(
                            "Vehículo {}: Error conectando a estación: {}. El timeout manejará el reintento.",
                            vehiculo_id, e
                        );
                    }
                }
            });
        } else {
            warn!(
                "Vehículo {}: No hay dirección de estación guardada para intentar carga",
                self.id
            );
        }
    }

    /// Limpia el estado de espera y cancela el temporizador de timeout.
    fn cancelar_timeout_carga(&mut self, ctx: &mut Context<Self>) {
        if let Some(handle) = self.timeout_handle {
            ctx.cancel_future(handle);
            self.timeout_handle = None;
        }
        self.esperando_respuesta_carga = false;
        self.reintentos_actuales = 0;
    }
}

impl Actor for Vehiculo {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        info!(
            "Vehículo {}: Iniciado (Compañía {})",
            self.id, self.compania_id
        );
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        info!("Vehículo {}: Detenido", self.id);
    }
}

impl Handler<MensajeRecibido<CommunicationHandler<Vehiculo>>> for Vehiculo {
    type Result = ();

    fn handle(
        &mut self,
        msg: MensajeRecibido<CommunicationHandler<Vehiculo>>,
        ctx: &mut Self::Context,
    ) {
        match msg.tipo_mensaje.as_str() {
            // Respuesta de dirección de estación desde el GPS
            "RespuestaDireccionEstacion" => {
                match serde_json::from_str::<RespuestaDireccionEstacion>(&msg.payload) {
                    Ok(respuesta) => {
                        if let Some(direccion) = respuesta.direccion {
                            info!("Vehículo {}: GPS indica estación - {}", self.id, direccion);

                            self.direccion_estacion_actual = Some(direccion);

                            if self.contador_cargas == 0 {
                                self.contador_cargas = 1;
                            }

                            self.iniciar_intento_carga(ctx);
                        } else {
                            warn!(
                                "Vehículo {}: No hay estaciones cercanas disponibles",
                                self.id
                            );
                            self.esperando_respuesta_carga = false;
                            self.cantidad_a_cargar = None;
                        }
                    }
                    Err(e) => {
                        error!(
                            "Vehículo {}: Error deserializando respuesta: {}",
                            self.id, e
                        );
                        self.esperando_respuesta_carga = false;
                    }
                }
            }
            // Respuesta de carga desde la estación
            "RespuestaCarga" => {
                self.cancelar_timeout_carga(ctx);

                match serde_json::from_str::<RespuestaCarga>(&msg.payload) {
                    Ok(resp) => {
                        if resp.aceptada {
                            info!("Vehículo {}: Carga ACEPTADA!", self.id);
                        } else {
                            warn!("Vehículo {}: Carga RECHAZADA", self.id);
                        }
                    }
                    Err(e) => error!(
                        "Vehículo {}: Error deserializando RespuestaCarga: {}",
                        self.id, e
                    ),
                }
            }
            // Mensaje desconocido
            _ => {
                warn!(
                    "Vehículo {}: Mensaje desconocido: {}",
                    self.id, msg.tipo_mensaje
                );
            }
        }
    }
}

impl Handler<ErrorConexion<CommunicationHandler<Vehiculo>>> for Vehiculo {
    type Result = ();
    fn handle(
        &mut self,
        msg: ErrorConexion<CommunicationHandler<Vehiculo>>,
        _: &mut Context<Self>,
    ) {
        error!(
            "Vehículo {}: Conexión perdida: {}",
            self.id, msg.descripcion
        );

        if let Some(handler_gps) = &self.handler_gps {
            if handler_gps == &msg.handler {
                warn!("Vehículo {}: Perdida conexión con GPS", self.id);
                self.handler_gps = None;
                self.esperando_respuesta_carga = false;
            }
        }
    }
}

impl Handler<ActorConnected<CommunicationHandler<Self>>> for Vehiculo {
    type Result = ();

    fn handle(
        &mut self,
        msg: ActorConnected<CommunicationHandler<Self>>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        warn!(
            "Vehículo {}: Tipo de actor inesperado conectado: {:?}",
            self.id, msg.actor_type
        );
    }
}

/// Mensaje interno para solicitar dirección de estación al GPS.
///
/// Inicia el proceso de carga especificando la cantidad de combustible.
pub struct SolicitarDireccionEstacionInterno {
    /// Cantidad de combustible a cargar en litros
    pub cantidad: f64,
}
impl actix::Message for SolicitarDireccionEstacionInterno {
    type Result = ();
}

impl Handler<SolicitarDireccionEstacionInterno> for Vehiculo {
    type Result = ();

    fn handle(&mut self, msg: SolicitarDireccionEstacionInterno, _: &mut Context<Self>) {
        if self.esperando_respuesta_carga {
            warn!(
                "Vehículo {} ya está ocupado en una operación de carga. Espere a que termine.",
                self.id
            );
            return;
        }

        if self.handler_gps.is_some() {
            info!(
                "Vehículo {}: Recibida orden manual. Iniciando carga de {} litros",
                self.id, msg.cantidad
            );
            self.contador_cargas = self.contador_cargas.saturating_add(1);
            self.esperando_respuesta_carga = true;
            self.cantidad_a_cargar = Some(msg.cantidad);
            self.solicitar_direccion();
        } else {
            error!("Error: No hay conexión con el GPS, no se puede iniciar la carga.");
        }
    }
}

/// Mensaje para configurar el handler de comunicación con el GPS.
pub struct SetHandlerGps(pub Addr<CommunicationHandler<Vehiculo>>);
impl actix::Message for SetHandlerGps {
    type Result = ();
}

impl Handler<SetHandlerGps> for Vehiculo {
    type Result = ();
    fn handle(&mut self, msg: SetHandlerGps, _: &mut Context<Self>) {
        info!("Vehículo {}: Handler GPS configurado", self.id);
        self.handler_gps = Some(msg.0);
    }
}

/// Mensaje para incrementar el contador de cargas del vehículo.
pub struct IncrementarContador;
impl actix::Message for IncrementarContador {
    type Result = u32;
}

impl Handler<IncrementarContador> for Vehiculo {
    type Result = u32;
    fn handle(&mut self, _msg: IncrementarContador, _: &mut Context<Self>) -> u32 {
        self.contador_cargas += 1;
        self.contador_cargas
    }
}

/// Mensaje para obtener el contador actual de cargas del vehículo.
pub struct ObtenerContador;
impl actix::Message for ObtenerContador {
    type Result = u32;
}

impl Handler<ObtenerContador> for Vehiculo {
    type Result = u32;
    fn handle(&mut self, _msg: ObtenerContador, _: &mut Context<Self>) -> u32 {
        if self.contador_cargas == 0 {
            self.contador_cargas = 1;
        }
        self.contador_cargas
    }
}
