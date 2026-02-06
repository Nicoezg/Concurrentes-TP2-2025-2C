//! Implementaciones del trait Actor y handlers principales para CuentaCompania.
//!
//! Este módulo implementa el ciclo de vida del actor, el enrutamiento de mensajes
//! y los handlers para conexiones y errores.

use crate::cuenta_compania::{CuentaCompania, IniciarEleccion, NotificarFalloNodo};
use crate::heartbeat_manager::HeartbeatManager;
use actix::{Actor, AsyncContext, Context, Handler};
use common::{
    ActorConnected, DetenerHeartbeats, EnviarMensaje, ErrorConexion, IniciarHeartbeats,
    MensajeRecibido, RolNodo,
};
use log::{debug, info, warn};
use network::CommunicationHandler;

impl Actor for CuentaCompania {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!("Actor CuentaCompania {} iniciado", self.id);

        // Inicializar HeartbeatManager
        let heartbeat_manager = HeartbeatManager::new(self.id, ctx.address()).start();
        self.heartbeat_manager = Some(heartbeat_manager);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        info!("Actor CuentaCompania {} detenido", self.id);
    }
}

impl Handler<MensajeRecibido<CommunicationHandler<CuentaCompania>>> for CuentaCompania {
    type Result = ();

    fn handle(
        &mut self,
        msg: MensajeRecibido<CommunicationHandler<CuentaCompania>>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        debug!(
            "Nodo {}: Mensaje recibido tipo '{}'",
            self.id, msg.tipo_mensaje
        );

        // Delegar al método handler correspondiente según el tipo de mensaje
        match msg.tipo_mensaje.as_str() {
            // Mensajes de heartbeat
            "HEARTBEAT_PING" => self.handle_heartbeat_ping(&msg.payload),
            "HEARTBEAT_PONG" => self.handle_heartbeat_pong(&msg.payload),

            // Mensajes de elección de líder
            "ELECTION" => self.handle_election_message(&msg.payload, ctx),
            "OK" => self.handle_ok_message(&msg.payload),
            "COORDINATOR" => self.handle_coordinator_message(&msg.payload),

            // Mensajes de replica
            "REPLICA" => self.handle_replica_message(&msg.payload),
            "ESTADO_INICIAL" => self.handle_estado_inicial(&msg.payload),
            "ESTADO_TRANSFERIDO_REPLICA" => self.handle_estado_transferido_replica(&msg.payload),

            // Mensajes de administrador
            "WHO_IS_LEADER" => self.handle_who_is_leader(&msg.payload, msg.direccion.clone()),

            // Mensajes de transacciones
            "NUEVA_TRANSACCION_ESTACION" => {
                self.handle_nueva_transaccion_estacion(&msg.payload, msg.direccion.clone())
            }
            "TRANSACCIONES" => {
                self.handle_transacciones_message(&msg.payload, msg.direccion.clone())
            }
            "SYNC_TRANSACCIONES" => self.handle_sync_transacciones(&msg.payload),
            "RESULTADOS_TRANSACCIONES" => self.handle_resultados_transacciones(&msg.payload),

            // Mensajes de consultas y configuración
            "CONSULTAR_SALDO_VEHICULO" => {
                self.handle_consultar_saldo_vehiculo(&msg.payload, msg.direccion.clone())
            }
            "CONSULTAR_SALDO_COMPANIA" => {
                self.handle_consultar_saldo_compania(&msg.payload, msg.direccion.clone())
            }
            "SOLICITAR_REPORTE_MENSUAL" => {
                self.handle_solicitar_reporte_mensual(&msg.payload, msg.direccion.clone())
            }

            "DEFINIR_LIMITE_COMPANIA" => self.handle_definir_limite_compania(&msg.payload),
            "DEFINIR_LIMITE_VEHICULO" => self.handle_definir_limite_vehiculo(&msg.payload),
            "SYNC_LIMITE_COMPANIA" => self.handle_sync_limite_compania(&msg.payload),
            "SYNC_LIMITE_VEHICULO" => self.handle_sync_limite_vehiculo(&msg.payload),
            _ => {
                warn!(
                    "Nodo {}: Mensaje desconocido: {}",
                    self.id, msg.tipo_mensaje
                );
            }
        }
    }
}

impl Handler<ActorConnected<CommunicationHandler<CuentaCompania>>> for CuentaCompania {
    type Result = ();

    fn handle(
        &mut self,
        msg: ActorConnected<CommunicationHandler<CuentaCompania>>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        use common::ActorType;

        match msg.actor_type {
            ActorType::NodoCompania => {
                info!(
                    "CuentaCompania {}: NodoCompania {} conectado desde {}",
                    self.id, msg.id, msg.direccion_remota
                );

                self.agregar_vecino(msg.id, msg.handler);
            }
            ActorType::Estacion => {
                info!(
                    "CuentaCompania {}: Estacion {} conectada desde {}",
                    self.id, msg.id, msg.direccion_remota
                );
            }
            ActorType::AdminCompania => {
                info!(
                    "CuentaCompania {}: AdminCompania {} conectada desde {}",
                    self.id, msg.id, msg.direccion_remota
                );
            }
            _ => {
                info!(
                    "CuentaCompania {}: Conexión inesperada de {:?} (id: {})",
                    self.id, msg.actor_type, msg.id
                );
            }
        }
    }
}

impl Handler<ErrorConexion<CommunicationHandler<CuentaCompania>>> for CuentaCompania {
    type Result = ();

    fn handle(
        &mut self,
        msg: ErrorConexion<CommunicationHandler<CuentaCompania>>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        warn!("Nodo {}: Error de conexión: {}", self.id, msg.descripcion);

        let mut nodo_caido: Option<u32> = None;
        for (id_vecino, handler) in &self.nodos_vecinos {
            if handler == &msg.handler {
                nodo_caido = Some(*id_vecino);
                break;
            }
        }

        if let Some(id_nodo_caido) = nodo_caido {
            warn!("Nodo {}: Vecino {} se desconectó", self.id, id_nodo_caido);

            self.nodos_vecinos.remove(&id_nodo_caido);

            if let Some(id_lider) = self.id_lider {
                if id_nodo_caido == id_lider {
                    warn!(
                        "Nodo {}: Líder {} caído, iniciando nueva elección",
                        self.id, id_lider
                    );
                    self.id_lider = None;
                    self.iniciar_eleccion(ctx);
                }
            }

            if self.rol == RolNodo::Lider {
                if let Some(id_replica) = self.id_replica {
                    if id_nodo_caido == id_replica {
                        warn!(
                            "Líder {}: Réplica {} caída, eligiendo nueva réplica",
                            self.id, id_replica
                        );

                        self.id_replica = None;

                        if let Some(&nueva_replica_id) = self.nodos_vecinos.keys().max() {
                            info!(
                                "Líder {}: Eligiendo a nodo {} como nueva réplica",
                                self.id, nueva_replica_id
                            );
                            self.id_replica = Some(nueva_replica_id);

                            if let Some(handler) = self.nodos_vecinos.get(&nueva_replica_id) {
                                handler.do_send(EnviarMensaje {
                                    tipo_mensaje: "REPLICA".to_string(),
                                    payload: serde_json::json!({"leader_id": self.id}).to_string(),
                                });

                                if let Ok(estado_serializado) =
                                    serde_json::to_string(&self.estado_global)
                                {
                                    handler.do_send(EnviarMensaje {
                                        tipo_mensaje: "ESTADO_INICIAL".to_string(),
                                        payload: estado_serializado,
                                    });
                                    info!(
                                        "Líder {}: Estado inicial enviado a nueva réplica {} ({} compañías)",
                                        self.id,
                                        nueva_replica_id,
                                        self.estado_global.len()
                                    );
                                }
                            }
                        } else {
                            warn!(
                                "Líder {}: No hay nodos disponibles para ser réplica",
                                self.id
                            );
                        }
                    }
                }
            }
        } else {
            warn!("Nodo {}: No se pudo identificar el vecino caído", self.id);
        }
    }
}

// Handler para iniciar elección manualmente
impl Handler<IniciarEleccion> for CuentaCompania {
    type Result = ();

    fn handle(&mut self, _msg: IniciarEleccion, ctx: &mut Self::Context) -> Self::Result {
        info!("Nodo {}: Recibido comando para iniciar elección", self.id);
        self.iniciar_eleccion(ctx);
    }
}

// Handlers para control de heartbeats
impl Handler<IniciarHeartbeats> for CuentaCompania {
    type Result = ();

    fn handle(&mut self, _msg: IniciarHeartbeats, _: &mut Self::Context) -> Self::Result {
        info!("Nodo {}: Comando para iniciar heartbeats", self.id);
        if let Some(hb_manager) = &self.heartbeat_manager {
            hb_manager.do_send(IniciarHeartbeats);
        }
    }
}

impl Handler<DetenerHeartbeats> for CuentaCompania {
    type Result = ();

    fn handle(&mut self, _msg: DetenerHeartbeats, _: &mut Self::Context) -> Self::Result {
        info!("Nodo {}: Comando para detener heartbeats", self.id);
        if let Some(hb_manager) = &self.heartbeat_manager {
            hb_manager.do_send(DetenerHeartbeats);
        }
    }
}

// Handler para notificaciones de fallo de nodo desde HeartbeatManager
impl Handler<NotificarFalloNodo> for CuentaCompania {
    type Result = ();

    fn handle(&mut self, _msg: NotificarFalloNodo, _ctx: &mut Self::Context) -> Self::Result {}
}
