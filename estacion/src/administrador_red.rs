//! Administrador de comunicación con el cluster de cuentas de compañía.
//!
//! Gestiona la conexión con los nodos del cluster, maneja reconexión automática
//! y coordina el envío y recepción de validaciones de transacciones.

use crate::persistencia::{EstadoTransaccion, LogTransaccion, TransactionLogger};
use crate::surtidor::Surtidor;
use actix::{Actor, ActorFutureExt, Addr, AsyncContext, Context, Handler, WrapFuture};
use common::{
    ActorConnected, ActorType, ErrorConexion, MensajeRecibido, RegistrarTransaccion,
    RegistroTransaccion, ResultadoValidacion,
};
use log::{error, info, warn};
use network::CommunicationHandler;
use rand::Rng;
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::TcpStream;

/// Tiempo de espera en segundos antes de intentar reconexión.
const TIEMPO_ESPERA_RECONEXION: u64 = 2;
/// Número máximo de intentos de reconexión antes de detener el sistema.
const MAX_INTENTOS_RECONEXION: u32 = 4;

/// Actor que gestiona la comunicación con el cluster de cuentas.
///
/// Maneja la conexión TCP con los nodos del cluster, implementa reconexión
/// automática, y coordina el envío de transacciones y recepción de resultados.
pub struct AdministradorDeRed {
    /// ID de la estación
    id_estacion: u32,
    /// Logger para persistencia de transacciones
    logger: TransactionLogger,
    /// Direcciones de los nodos del cluster
    direcciones_cluster: Vec<String>,
    /// Handler de comunicación con el nodo actualmente conectado
    nodo_activo: Option<Addr<CommunicationHandler<Self>>>,
    /// Índice del nodo al que se está conectando o conectado
    indice_nodo_objetivo: usize,
    /// Contador de intentos de reconexión
    intentos_reconexion: u32,
    /// Surtidores registrados
    surtidores: HashMap<u32, Addr<Surtidor>>,
    /// Mapeo de ID de transacción a ID de surtidor
    transaccion_a_surtidor: HashMap<String, u32>,
}

impl AdministradorDeRed {
    /// Crea un nuevo administrador de red para una estación.
    pub fn new(id_estacion: u32) -> Self {
        let logger = TransactionLogger::new(id_estacion)
            .expect("Error fatal: No se pudo inicializar o leer el log de transacciones");

        Self {
            id_estacion,
            logger,
            direcciones_cluster: Vec::new(),
            nodo_activo: None,
            indice_nodo_objetivo: 0,
            intentos_reconexion: 0,
            surtidores: HashMap::new(),
            transaccion_a_surtidor: HashMap::new(),
        }
    }

    /// Intenta establecer conexión TCP con el nodo del cluster seleccionado.
    fn intentar_conectar(&mut self, ctx: &mut Context<Self>) {
        if self.nodo_activo.is_some() {
            return;
        }

        if self.direcciones_cluster.is_empty() {
            warn!("AdministradorDeRed: No hay direcciones de cluster configuradas para conectar.");
            return;
        }

        let addr_str = self.direcciones_cluster[self.indice_nodo_objetivo].clone();
        let estacion_id = self.id_estacion;
        let mi_direccion = ctx.address();

        info!(
            "AdministradorDeRed: Intentando conectar con nodo {} (índice {})...",
            addr_str, self.indice_nodo_objetivo
        );

        let connect_future = async move {
            TcpStream::connect(&addr_str)
                .await
                .map(|stream| (stream, addr_str))
        }
        .into_actor(self)
        .map(move |res, act, _ctx| match res {
            Ok((stream, addr_conectada)) => {
                info!(
                    "AdministradorDeRed: ¡Conexión establecida con {}!",
                    addr_conectada
                );

                let handler = CommunicationHandler::new_initiator(
                    stream,
                    addr_conectada,
                    mi_direccion,
                    ActorType::Estacion,
                    estacion_id,
                )
                .start();

                act.nodo_activo = Some(handler);
                act.intentos_reconexion = 0;
                act.procesar_cola_pendiente(_ctx);
            }
            Err(e) => {
                error!(
                    "AdministradorDeRed: Falló conexión con {}: {}",
                    act.direcciones_cluster[act.indice_nodo_objetivo], e
                );
                act.programar_reconexion(_ctx);
            }
        });

        ctx.spawn(connect_future);
    }

    /// Programa un intento de reconexión seleccionando un nodo aleatorio.
    ///
    /// Detiene los intentos si se alcanza MAX_INTENTOS_RECONEXION.
    fn programar_reconexion(&mut self, ctx: &mut Context<Self>) {
        self.nodo_activo = None;
        self.intentos_reconexion += 1;

        // Verificar si se alcanzó el máximo de intentos
        if self.intentos_reconexion >= MAX_INTENTOS_RECONEXION {
            error!(
                "❌ AdministradorDeRed: ESTACIÓN {} SIN CONEXIÓN - Se alcanzó el máximo de intentos ({}/{})",
                self.id_estacion,
                self.intentos_reconexion,
                MAX_INTENTOS_RECONEXION
            );
            error!(
                "❌ AdministradorDeRed: La estación {} NO puede conectarse al cluster. Sistema detenido.",
                self.id_estacion
            );
            return; // No programar más intentos
        }

        if self.direcciones_cluster.len() > 1 {
            // Elegir un nodo aleatorio que no sea el actual
            let mut rng = rand::thread_rng();
            let nodos_disponibles: Vec<usize> = (0..self.direcciones_cluster.len())
                .filter(|&i| i != self.indice_nodo_objetivo)
                .collect();

            if !nodos_disponibles.is_empty() {
                let idx = rng.gen_range(0..nodos_disponibles.len());
                self.indice_nodo_objetivo = nodos_disponibles[idx];

                info!(
                    "AdministradorDeRed: Intento {}/{} - Cambiando a nodo aleatorio {} (índice {})",
                    self.intentos_reconexion,
                    MAX_INTENTOS_RECONEXION,
                    self.direcciones_cluster[self.indice_nodo_objetivo],
                    self.indice_nodo_objetivo
                );
            }
        }

        ctx.run_later(Duration::from_secs(TIEMPO_ESPERA_RECONEXION), |act, ctx| {
            act.intentar_conectar(ctx);
        });
    }

    /// Procesa la cola de transacciones pendientes reenviándolas al cluster.
    ///
    /// Se ejecuta al reconectar al cluster (patrón Store and Forward).
    fn procesar_cola_pendiente(&mut self, ctx: &mut Context<Self>) {
        if self.nodo_activo.is_none() {
            return;
        }

        info!("AdministradorDeRed: Buscando transacciones pendientes...");

        let pendientes = self
            .logger
            .get_transacciones_por_estado(EstadoTransaccion::Pendiente);

        if pendientes.is_empty() {
            info!("AdministradorDeRed: No hay transacciones pendientes.");
            return;
        }

        let ahora = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        info!(
            "AdministradorDeRed: Re-enviando {} transacciones pendientes...",
            pendientes.len()
        );

        for log in pendientes {
            let mut log_actualizado = log.clone();
            log_actualizado.timestamp_envio = ahora;
            let _ = self.logger.log(&log_actualizado);
            self.enviar_transaccion_a_cluster(log.transaccion, ctx);
        }
    }

    /// Envía una transacción al nodo activo del cluster.
    fn enviar_transaccion_a_cluster(
        &mut self,
        transaccion: RegistroTransaccion,
        _ctx: &mut Context<Self>,
    ) {
        if let Some(node_handler) = &self.nodo_activo {
            let transaccion_msg = common::NuevaTransaccionEstacion {
                transaccion: transaccion.clone(),
            };

            match serde_json::to_string(&transaccion_msg) {
                Ok(payload) => {
                    node_handler.do_send(common::EnviarMensaje {
                        tipo_mensaje: "NUEVA_TRANSACCION_ESTACION".to_string(),
                        payload,
                    });

                    info!(
                        "AdministradorDeRed: Transacción {} enviada al cluster.",
                        transaccion.id
                    );
                }
                Err(e) => {
                    error!("AdministradorDeRed: Error serializando msg: {}", e);
                }
            }
        } else {
            warn!("AdministradorDeRed: Sin conexión al cluster. Se guardó en log y se enviará al reconectar.");
        }
    }
}

impl Actor for AdministradorDeRed {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        info!(
            "AdministradorDeRed: Iniciado para estación {}",
            self.id_estacion
        );
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        info!("AdministradorDeRed: Detenido");
    }
}

impl Handler<MensajeRecibido<CommunicationHandler<AdministradorDeRed>>> for AdministradorDeRed {
    type Result = ();

    fn handle(
        &mut self,
        msg: MensajeRecibido<CommunicationHandler<AdministradorDeRed>>,
        _: &mut Self::Context,
    ) -> Self::Result {
        match msg.tipo_mensaje.as_str() {
            // Resultado de validación de transacción desde el cluster
            "RESULTADO_TRANSACCION_ESTACION" => {
                if let Ok(resultado) =
                    serde_json::from_str::<common::ResultadoTransaccionEstacion>(&msg.payload)
                {
                    let id = resultado.transaccion.id.clone();
                    let aprobada = resultado.aprobada;

                    info!(
                        "AdministradorDeRed: Recibido resultado para TX {}: {}",
                        id,
                        if aprobada { "APROBADA" } else { "RECHAZADA" }
                    );

                    let log_entry = LogTransaccion {
                        id: id.clone(),
                        estado: EstadoTransaccion::Confirmada,
                        transaccion: resultado.transaccion,
                        timestamp_envio: 0,
                        aprobada: Some(aprobada),
                    };

                    if let Err(e) = self.logger.log(&log_entry) {
                        error!("AdministradorDeRed: Error log TX {}: {}", id, e);
                    }

                    if let Some(&surtidor_id) = self.transaccion_a_surtidor.get(&id) {
                        if let Some(surtidor) = self.surtidores.get(&surtidor_id) {
                            info!(
                                "AdministradorDeRed: Notificando a surtidor {} (TX {})",
                                surtidor_id, id
                            );
                            surtidor.do_send(ResultadoValidacion { aprobada });
                        } else {
                            warn!(
                                "AdministradorDeRed: Surtidor {} no registrado activamente.",
                                surtidor_id
                            );
                        }
                    } else {
                        warn!("AdministradorDeRed: TX {} confirmada, pero no hay surtidor esperando en RAM. Se espera reintento del vehículo.", id);
                    }
                } else {
                    error!(
                        "AdministradorDeRed: Error deserializando RESULTADO_TRANSACCION_ESTACION"
                    );
                }
            }
            _ => {
                warn!(
                    "AdministradorDeRed: Mensaje desconocido: {}",
                    msg.tipo_mensaje
                );
            }
        }
    }
}

impl Handler<ErrorConexion<CommunicationHandler<AdministradorDeRed>>> for AdministradorDeRed {
    type Result = ();

    fn handle(
        &mut self,
        msg: ErrorConexion<CommunicationHandler<AdministradorDeRed>>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        error!(
            "AdministradorDeRed: Error conexión cluster: {}",
            msg.descripcion
        );

        self.programar_reconexion(ctx);
    }
}

impl Handler<ActorConnected<CommunicationHandler<AdministradorDeRed>>> for AdministradorDeRed {
    type Result = ();

    fn handle(
        &mut self,
        msg: ActorConnected<CommunicationHandler<AdministradorDeRed>>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        warn!(
            "AdministradorDeRed: Conexión entrante inesperada (ID: {})",
            msg.id
        );
    }
}

impl Handler<RegistrarTransaccion> for AdministradorDeRed {
    type Result = ();

    fn handle(&mut self, mut msg: RegistrarTransaccion, ctx: &mut Self::Context) -> Self::Result {
        let id = format!(
            "{}-{}-{}",
            msg.transaccion.vehiculo_id,
            msg.transaccion.compania_id,
            msg.transaccion.contador_carga
        );
        msg.transaccion.id = id.clone();

        self.transaccion_a_surtidor
            .insert(id.clone(), msg.transaccion.surtidor_id);

        if let Some(transaccion_existente) = self.logger.get_transaccion(&id) {
            match transaccion_existente.estado {
                EstadoTransaccion::Pendiente => {
                    info!(
                        "AdministradorDeRed: TX {} REINTENTO detectado (Estado: PENDIENTE). Enlace actualizado a surtidor {}.",
                        id, msg.transaccion.surtidor_id
                    );

                    if self.nodo_activo.is_some() {
                        info!(
                            "AdministradorDeRed: TX {} - Reenviando al cluster (ahora conectado).",
                            id
                        );
                        self.enviar_transaccion_a_cluster(msg.transaccion, ctx);
                    } else {
                        info!(
                            "AdministradorDeRed: TX {} - Esperando conexión al cluster...",
                            id
                        );
                    }
                }
                EstadoTransaccion::Confirmada => {
                    if let Some(aprobada) = transaccion_existente.aprobada {
                        info!(
                            "AdministradorDeRed: TX {} REINTENTO detectado (Estado: CONFIRMADA/{}). Respondiendo ya a surtidor {}.",
                            id, aprobada, msg.transaccion.surtidor_id
                        );

                        if let Some(surtidor) = self.surtidores.get(&msg.transaccion.surtidor_id) {
                            surtidor.do_send(ResultadoValidacion { aprobada });
                        }
                    }
                }
            }
            return;
        }

        info!(
            "AdministradorDeRed: Nueva transacción {}. Surtidor {}.",
            id, msg.transaccion.surtidor_id
        );

        let log_entry = LogTransaccion {
            id: id.clone(),
            estado: EstadoTransaccion::Pendiente,
            transaccion: msg.transaccion.clone(),
            timestamp_envio: 0,
            aprobada: None,
        };

        if let Err(e) = self.logger.log(&log_entry) {
            error!(
                "AdministradorDeRed: Error crítico escribiendo disco TX {}: {}",
                id, e
            );
            if let Some(surtidor) = self.surtidores.get(&msg.transaccion.surtidor_id) {
                surtidor.do_send(ResultadoValidacion { aprobada: false });
            }
            return;
        }

        self.enviar_transaccion_a_cluster(msg.transaccion, ctx);
    }
}

/// Mensaje para configurar las direcciones de los nodos del cluster.
#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct ConfigurarDireccionesCluster(pub Vec<String>);

impl Handler<ConfigurarDireccionesCluster> for AdministradorDeRed {
    type Result = ();

    fn handle(
        &mut self,
        msg: ConfigurarDireccionesCluster,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        self.direcciones_cluster = msg.0;
        info!("AdministradorDeRed: Direcciones cluster actualizadas. Conectando...");
        self.indice_nodo_objetivo = 0;
        self.nodo_activo = None;
        self.intentar_conectar(ctx);
    }
}

/// Mensaje para registrar un surtidor en el administrador.
#[derive(actix::Message)]
#[rtype(result = "()")]
pub struct RegistrarSurtidor {
    /// ID del surtidor
    pub surtidor_id: u32,
    /// Dirección del actor surtidor
    pub addr: Addr<Surtidor>,
}

impl Handler<RegistrarSurtidor> for AdministradorDeRed {
    type Result = ();

    fn handle(&mut self, msg: RegistrarSurtidor, _ctx: &mut Self::Context) -> Self::Result {
        self.surtidores.insert(msg.surtidor_id, msg.addr);
        info!(
            "AdministradorDeRed: Surtidor {} registrado",
            msg.surtidor_id
        );
    }
}
