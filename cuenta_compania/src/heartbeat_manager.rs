//! Sistema de heartbeats para detección de fallos en el cluster.
//!
//! Implementa un mecanismo de ping/pong periódico entre nodos para detectar
//! fallos y notificar al actor principal cuando un nodo deja de responder.

use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, SpawnHandle};
use common::{
    DetenerHeartbeats, EnviarMensaje, HeartbeatConfig, HeartbeatPing, HeartbeatPong,
    HeartbeatState, IniciarHeartbeats,
};
use log::{debug, error, info, warn};
use network::CommunicationHandler;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Mensaje para agregar un nodo al sistema de heartbeats.
#[derive(Message)]
#[rtype(result = "()")]
pub struct AgregarNodo {
    /// ID del nodo a agregar
    pub id_nodo: u32,
    /// Handler de comunicación con el nodo
    pub handler: Addr<CommunicationHandler<crate::cuenta_compania::CuentaCompania>>,
}

/// Mensaje para remover un nodo del sistema de heartbeats.
#[derive(Message)]
#[rtype(result = "()")]
pub struct RemoverNodo {
    /// ID del nodo a remover
    pub id_nodo: u32,
}

/// Mensaje para procesar un ping recibido.
#[derive(Message)]
#[rtype(result = "()")]
pub struct ProcesarPingHeartbeat {
    /// Ping recibido
    pub ping: HeartbeatPing,
}

/// Mensaje para procesar un pong recibido.
#[derive(Message)]
#[rtype(result = "()")]
pub struct ProcesarPongHeartbeat {
    /// Pong recibido
    pub pong: HeartbeatPong,
}

/// Actor que gestiona el sistema de heartbeats del cluster.
///
/// Envía pings periódicos a todos los nodos, verifica pongs recibidos y
/// detecta timeouts para notificar fallos al actor CuentaCompania.
pub struct HeartbeatManager {
    /// ID del nodo local
    id_nodo: u32,
    /// Configuración del sistema de heartbeats
    config: HeartbeatConfig,
    /// Estados de heartbeat para cada nodo
    estados_heartbeats: HashMap<u32, HeartbeatState>,
    /// Handlers de comunicación con otros nodos
    handlers_nodo: HashMap<u32, Addr<CommunicationHandler<crate::cuenta_compania::CuentaCompania>>>,
    /// Handle del timer para enviar pings
    handle_ping: Option<SpawnHandle>,
    /// Handle del timer para verificar timeouts
    handle_check: Option<SpawnHandle>,
    /// Flag que indica si los heartbeats están activos
    activo: bool,
    /// Dirección del actor CuentaCompania para notificar fallos
    cuenta_compania_addr: Addr<crate::cuenta_compania::CuentaCompania>,
}

impl HeartbeatManager {
    /// Crea un nuevo HeartbeatManager
    pub fn new(
        id_nodo: u32,
        cuenta_compania_addr: Addr<crate::cuenta_compania::CuentaCompania>,
    ) -> Self {
        Self {
            id_nodo,
            config: HeartbeatConfig::default(),
            estados_heartbeats: HashMap::new(),
            handlers_nodo: HashMap::new(),
            handle_ping: None,
            handle_check: None,
            activo: false,
            cuenta_compania_addr,
        }
    }

    /// Obtiene el timestamp actual en milisegundos
    fn get_timestamp(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Inicia el sistema de heartbeats
    fn iniciar_heartbeats(&mut self, ctx: &mut Context<Self>) {
        if self.activo {
            debug!(
                "HeartbeatManager {}: Heartbeats ya están activos",
                self.id_nodo
            );
            return;
        }

        info!(
            "HeartbeatManager {}: Iniciando sistema de heartbeats",
            self.id_nodo
        );
        self.activo = true;

        // Iniciar timer para enviar pings
        self.inciar_timers_ping(ctx);

        // Iniciar timer para verificar timeouts
        self.iniciar_timer_check(ctx);
    }

    /// Detiene el sistema de heartbeats
    fn detener_heartbeats(&mut self, ctx: &mut Context<Self>) {
        if !self.activo {
            return;
        }

        info!(
            "HeartbeatManager {}: Deteniendo sistema de heartbeats",
            self.id_nodo
        );
        self.activo = false;

        if let Some(handle) = self.handle_ping.take() {
            ctx.cancel_future(handle);
        }
        if let Some(handle) = self.handle_check.take() {
            ctx.cancel_future(handle);
        }
    }

    /// Inicia el timer para enviar pings
    fn inciar_timers_ping(&mut self, ctx: &mut Context<Self>) {
        let interval = self.config.ping_interval;

        debug!(
            "HeartbeatManager {}: Iniciando pings cada {:?}",
            self.id_nodo, interval
        );

        let handle = ctx.run_interval(interval, move |actor, _ctx| {
            if actor.activo {
                actor.enviar_pings_heartbeat();
            }
        });

        self.handle_ping = Some(handle);
    }

    /// Inicia el timer para verificar timeouts
    fn iniciar_timer_check(&mut self, ctx: &mut Context<Self>) {
        let check_interval = Duration::from_millis(1000); // Verificar cada segundo

        debug!(
            "HeartbeatManager {}: Iniciando detector de fallos cada {:?}",
            self.id_nodo, check_interval
        );

        let handle = ctx.run_interval(check_interval, move |actor, ctx| {
            if actor.activo {
                actor.verificar_timeouts(ctx);
            }
        });

        self.handle_check = Some(handle);
    }

    /// Envía pings a todos los nodos conocidos
    fn enviar_pings_heartbeat(&mut self) {
        let timestamp = self.get_timestamp();

        let ping_msg = serde_json::to_string(&HeartbeatPing {
            id_nodo: self.id_nodo,
            timestamp,
        })
        .unwrap_or_default();

        for handler in self.handlers_nodo.values() {
            handler.do_send(EnviarMensaje {
                tipo_mensaje: "HEARTBEAT_PING".to_string(),
                payload: ping_msg.clone(),
            });
        }
    }

    /// Verifica si algún nodo ha excedido el timeout
    fn verificar_timeouts(&mut self, ctx: &mut Context<Self>) {
        let mut failed_nodes = Vec::new();
        let timeout = self.config.failure_timeout;

        // Verificar cada nodo por timeout
        for (id_nodo, state) in &mut self.estados_heartbeats {
            if state.is_timed_out(timeout) && state.is_alive {
                warn!(
                    "HeartbeatManager {}: Detectado timeout para nodo {} (timeout: {:?})",
                    self.id_nodo, id_nodo, timeout
                );
                state.mark_failed();
                failed_nodes.push(*id_nodo);
            }
        }

        // Manejar nodos fallidos
        for id_nodo in failed_nodes {
            self.handle_falla_nodo(id_nodo, ctx);
        }
    }

    /// Maneja el fallo de un nodo
    fn handle_falla_nodo(&mut self, id_nodo: u32, _ctx: &mut Context<Self>) {
        error!(
            "HeartbeatManager {}: Detectada falla del nodo {}",
            self.id_nodo, id_nodo
        );

        // Remover el nodo de nuestras estructuras
        self.handlers_nodo.remove(&id_nodo);
        self.estados_heartbeats.remove(&id_nodo);

        info!(
            "HeartbeatManager {}: Removido nodo caído {} de la lista",
            self.id_nodo, id_nodo
        );

        // Notificar a CuentaCompania sobre el fallo
        self.cuenta_compania_addr
            .do_send(crate::cuenta_compania::NotificarFalloNodo {});
    }

    /// Procesa un ping recibido
    fn procesar_ping(&mut self, ping: &HeartbeatPing) {
        // Actualizar estado de heartbeat
        if let Some(state) = self.estados_heartbeats.get_mut(&ping.id_nodo) {
            state.update_ping_received();
        } else {
            // Nuevo nodo, agregar estado
            self.estados_heartbeats
                .insert(ping.id_nodo, HeartbeatState::new());
        }

        // Enviar respuesta pong
        self.enviar_pong_heartbeat(ping.id_nodo);
    }

    /// Procesa un pong recibido
    fn procesar_pong(&mut self, pong: &HeartbeatPong) {
        // Actualizar estado de heartbeat
        if let Some(state) = self.estados_heartbeats.get_mut(&pong.id_nodo) {
            state.update_ping_received();
        } else {
            // Nuevo nodo, agregar estado
            self.estados_heartbeats
                .insert(pong.id_nodo, HeartbeatState::new());
        }
    }

    /// Envía una respuesta pong
    fn enviar_pong_heartbeat(&self, target_id: u32) {
        if let Some(handler) = self.handlers_nodo.get(&target_id) {
            let pong = serde_json::to_string(&HeartbeatPong {
                id_nodo: self.id_nodo,
                timestamp: self.get_timestamp(),
            })
            .unwrap_or_default();

            handler.do_send(EnviarMensaje {
                tipo_mensaje: "HEARTBEAT_PONG".to_string(),
                payload: pong,
            });
        }
    }
}

impl Actor for HeartbeatManager {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!("HeartbeatManager {} iniciado", self.id_nodo);
        // Iniciar heartbeats automáticamente
        self.iniciar_heartbeats(ctx);
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        info!("HeartbeatManager {} detenido", self.id_nodo);
        self.detener_heartbeats(ctx);
    }
}

// Implementación de handlers para mensajes

impl Handler<AgregarNodo> for HeartbeatManager {
    type Result = ();

    fn handle(&mut self, msg: AgregarNodo, _: &mut Self::Context) -> Self::Result {
        info!(
            "HeartbeatManager {}: Agregando nodo {}",
            self.id_nodo, msg.id_nodo
        );
        self.handlers_nodo.insert(msg.id_nodo, msg.handler);
        self.estados_heartbeats
            .insert(msg.id_nodo, HeartbeatState::new());
        debug!(
            "HeartbeatManager {}: Estado de heartbeat inicializado para nodo {}",
            self.id_nodo, msg.id_nodo
        );
    }
}

impl Handler<RemoverNodo> for HeartbeatManager {
    type Result = ();

    fn handle(&mut self, msg: RemoverNodo, _: &mut Self::Context) -> Self::Result {
        info!(
            "HeartbeatManager {}: Removiendo nodo {}",
            self.id_nodo, msg.id_nodo
        );
        self.handlers_nodo.remove(&msg.id_nodo);
        self.estados_heartbeats.remove(&msg.id_nodo);
    }
}

impl Handler<IniciarHeartbeats> for HeartbeatManager {
    type Result = ();

    fn handle(&mut self, _: IniciarHeartbeats, ctx: &mut Self::Context) -> Self::Result {
        info!(
            "HeartbeatManager {}: Comando para iniciar heartbeats",
            self.id_nodo
        );
        self.iniciar_heartbeats(ctx);
    }
}

impl Handler<DetenerHeartbeats> for HeartbeatManager {
    type Result = ();

    fn handle(&mut self, _: DetenerHeartbeats, ctx: &mut Self::Context) -> Self::Result {
        info!(
            "HeartbeatManager {}: Comando para detener heartbeats",
            self.id_nodo
        );
        self.detener_heartbeats(ctx);
    }
}

impl Handler<ProcesarPingHeartbeat> for HeartbeatManager {
    type Result = ();

    fn handle(&mut self, msg: ProcesarPingHeartbeat, _: &mut Self::Context) -> Self::Result {
        self.procesar_ping(&msg.ping);
    }
}

impl Handler<ProcesarPongHeartbeat> for HeartbeatManager {
    type Result = ();

    fn handle(&mut self, msg: ProcesarPongHeartbeat, _: &mut Self::Context) -> Self::Result {
        self.procesar_pong(&msg.pong);
    }
}
