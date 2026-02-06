//! Actor que gestiona una estación de servicio.
//!
//! Coordina los surtidores, gestiona la cola de vehículos en espera y
//! comunica los resultados de las cargas a los vehículos.

use crate::surtidor::Surtidor;
use actix::Addr;
use actix::{Actor, Context, Handler};
use common::{
    ActorConnected, CargaCompletada, EnviarMensaje, ErrorConexion, IniciarCarga, MensajeRecibido,
    RespuestaCarga, SolicitarCarga,
};
use log::{error, info, warn};
use network::CommunicationHandler;
use std::collections::{HashMap, VecDeque};

/// Solicitud de carga pendiente de asignación a surtidor.
#[derive(Debug, Clone)]
pub struct SolicitudCargaPendiente {
    /// ID del vehículo solicitante
    pub vehiculo_id: u32,
    /// ID de la compañía del vehículo
    pub compania_id: u32,
    /// Cantidad de combustible solicitada
    pub cantidad: f64,
    /// Contador de carga para idempotencia
    pub contador_carga: u32,
    /// Dirección para comunicación con el vehículo
    pub addr_vehiculo: Addr<CommunicationHandler<EstacionDeServicio>>,
}

/// Actor que gestiona una estación de servicio.
///
/// Mantiene el registro de surtidores disponibles y gestiona una cola de
/// vehículos esperando ser atendidos.
pub struct EstacionDeServicio {
    /// ID único de la estación
    id: u32,
    /// Surtidores registrados con su estado (dirección, disponible)
    surtidores: HashMap<u32, (Addr<Surtidor>, bool)>,
    /// Cola de vehículos esperando un surtidor libre
    vehiculos_en_espera: VecDeque<SolicitudCargaPendiente>,
}

impl EstacionDeServicio {
    /// Crea una nueva estación con el ID especificado.
    pub fn new(id: u32) -> Self {
        Self {
            id,
            surtidores: HashMap::new(),
            vehiculos_en_espera: VecDeque::new(),
        }
    }

    /// Agrega un surtidor a la estación.
    pub fn agregar_surtidor(&mut self, surtidor_id: u32, addr: Addr<Surtidor>) {
        self.surtidores.insert(surtidor_id, (addr, true));
        info!("Estación {}: Surtidor {} agregado", self.id, surtidor_id);
    }

    /// Busca un surtidor libre y lo marca como ocupado.
    ///
    /// Retorna el ID del surtidor y su dirección, o None si todos están ocupados.
    fn encontrar_surtidor_libre(&mut self) -> Option<(u32, Addr<Surtidor>)> {
        for (id, (addr, libre)) in self.surtidores.iter_mut() {
            if *libre {
                *libre = false;
                info!(
                    "Estación {}: Surtidor {} asignado y marcado como ocupado",
                    self.id, id
                );
                return Some((*id, addr.clone()));
            }
        }
        None
    }

    /// Procesa el siguiente vehículo en la cola de espera si hay un surtidor libre.
    fn procesar_siguiente_vehiculo(&mut self) {
        if let Some(solicitud) = self.vehiculos_en_espera.pop_front() {
            if let Some((surtidor_id, addr_surtidor)) = self.encontrar_surtidor_libre() {
                info!(
                    "Estación {}: Asignando vehículo {} (de la cola) al surtidor {}",
                    self.id, solicitud.vehiculo_id, surtidor_id
                );

                addr_surtidor.do_send(IniciarCarga {
                    vehiculo_id: solicitud.vehiculo_id,
                    compania_id: solicitud.compania_id,
                    cantidad: solicitud.cantidad,
                    contador_carga: solicitud.contador_carga,
                    addr_vehiculo: solicitud.addr_vehiculo,
                });
            } else {
                info!(
                    "Estación {}: No hay surtidores libres, vehículo {} sigue en espera",
                    self.id, solicitud.vehiculo_id
                );
                self.vehiculos_en_espera.push_front(solicitud);
            }
        }
    }

    /// Envía una respuesta de carga al vehículo.
    fn enviar_respuesta_vehiculo(
        &mut self,
        addr_vehiculo: Addr<CommunicationHandler<Self>>,
        aceptada: bool,
    ) {
        let respuesta = RespuestaCarga { aceptada };

        match serde_json::to_string(&respuesta) {
            Ok(payload) => {
                addr_vehiculo.do_send(EnviarMensaje {
                    tipo_mensaje: "RespuestaCarga".to_string(),
                    payload,
                });
                info!(
                    "Estación {}: Respuesta enviada a vehículo: {}",
                    self.id,
                    if aceptada { "ACEPTADA" } else { "RECHAZADA" }
                );
            }
            Err(e) => {
                error!(
                    "Estación {}: Error serializando RespuestaCarga: {}",
                    self.id, e
                );
            }
        }
    }
}

impl Actor for EstacionDeServicio {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        info!("Estación {}: Estación de Servicio iniciada", self.id);
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        info!("Estación {}: Estación de Servicio detenida", self.id);
    }
}

impl Handler<MensajeRecibido<CommunicationHandler<EstacionDeServicio>>> for EstacionDeServicio {
    type Result = ();

    fn handle(
        &mut self,
        msg: MensajeRecibido<CommunicationHandler<EstacionDeServicio>>,
        _: &mut Self::Context,
    ) -> Self::Result {
        match msg.tipo_mensaje.as_str() {
            "SolicitarCarga" => match serde_json::from_str::<SolicitarCarga>(&msg.payload) {
                Ok(solicitud) => {
                    info!(
                        "Estación {}: Solicitud de carga de vehículo {} ({} litros, contador: {}) desde {}",
                        self.id, solicitud.vehiculo_id, solicitud.cantidad, solicitud.contador_carga, msg.direccion_remota
                    );

                    if let Some((surtidor_id, addr_surtidor)) = self.encontrar_surtidor_libre() {
                        info!(
                            "Estación {}: Asignando vehículo {} al surtidor {}",
                            self.id, solicitud.vehiculo_id, surtidor_id
                        );

                        addr_surtidor.do_send(IniciarCarga {
                            vehiculo_id: solicitud.vehiculo_id,
                            compania_id: solicitud.compania_id,
                            cantidad: solicitud.cantidad,
                            contador_carga: solicitud.contador_carga,
                            addr_vehiculo: msg.direccion.clone(),
                        });
                    } else {
                        info!(
                            "Estación {}: Todos los surtidores ocupados. Vehículo {} en espera",
                            self.id, solicitud.vehiculo_id
                        );

                        self.vehiculos_en_espera.push_back(SolicitudCargaPendiente {
                            vehiculo_id: solicitud.vehiculo_id,
                            compania_id: solicitud.compania_id,
                            cantidad: solicitud.cantidad,
                            contador_carga: solicitud.contador_carga,
                            addr_vehiculo: msg.direccion.clone(),
                        });
                    }
                }
                Err(e) => {
                    error!(
                        "Estación {}: Error deserializando SolicitarCarga: {}",
                        self.id, e
                    );
                }
            },
            _ => {
                warn!(
                    "Estación {}: Tipo de mensaje desconocido: {}",
                    self.id, msg.tipo_mensaje
                );
            }
        }
    }
}

impl Handler<ErrorConexion<CommunicationHandler<EstacionDeServicio>>> for EstacionDeServicio {
    type Result = ();

    fn handle(
        &mut self,
        msg: ErrorConexion<CommunicationHandler<EstacionDeServicio>>,
        _: &mut Self::Context,
    ) -> Self::Result {
        error!(
            "Estación {}: Error de conexión: {}",
            self.id, msg.descripcion
        );
    }
}

impl Handler<CargaCompletada<CommunicationHandler<EstacionDeServicio>>> for EstacionDeServicio {
    type Result = ();

    fn handle(
        &mut self,
        msg: CargaCompletada<CommunicationHandler<EstacionDeServicio>>,
        _: &mut Self::Context,
    ) -> Self::Result {
        if msg.exitosa {
            info!(
                "Estación {}: Un vehículo puede retirarse - Carga completada exitosamente",
                self.id
            );
            self.enviar_respuesta_vehiculo(msg.addr_vehiculo, true);
        } else {
            warn!(
                "Estación {}: Un vehículo puede retirarse - Carga rechazada por el cluster",
                self.id
            );
            self.enviar_respuesta_vehiculo(msg.addr_vehiculo, false);
        }

        info!(
            "Estación {}: Surtidor {} ahora libre",
            self.id, msg.surtidor_id
        );

        if let Some((_, libre)) = self.surtidores.get_mut(&msg.surtidor_id) {
            *libre = true;
        }

        self.procesar_siguiente_vehiculo();
    }
}

impl Handler<ActorConnected<CommunicationHandler<Self>>> for EstacionDeServicio {
    type Result = ();

    fn handle(
        &mut self,
        msg: ActorConnected<CommunicationHandler<Self>>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        use common::ActorType;

        match msg.actor_type {
            ActorType::Vehiculo => {
                info!(
                    "EstacionDeServicio {}: Auto {} conectado desde {}",
                    self.id, msg.id, msg.direccion_remota
                );
            }
            _ => {
                info!(
                    "EstacionDeServicio: Conexión inesperada de {:?} (id: {})",
                    msg.actor_type, msg.id
                );
            }
        }
    }
}
