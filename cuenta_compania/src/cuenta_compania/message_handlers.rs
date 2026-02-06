//! Handlers de mensajes para CuentaCompania.
//!
//! Procesa todos los tipos de mensajes recibidos: heartbeats, elección de líder,
//! transacciones, consultas administrativas y sincronización de estado.

use crate::cuenta_compania::CuentaCompania;
use crate::heartbeat_manager::{ProcesarPingHeartbeat, ProcesarPongHeartbeat};
use actix::{Addr, Context};
use common::{
    ConsultarSaldoCompania, ConsultarSaldoVehiculo, DefinirLimiteCompania, DefinirLimiteVehiculo,
    EnviarMensaje, EstadoCompania, HeartbeatPing, HeartbeatPong, NuevaTransaccionEstacion,
    RegistroTransaccion, ReporteMensual, ResponderSaldo, RolNodo, SolicitarReporteMensual,
    TipoConsulta,
};
use log::{error, info, warn};
use network::CommunicationHandler;
use std::collections::HashMap;

impl CuentaCompania {
    /// Procesa un mensaje HEARTBEAT_PING delegándolo al HeartbeatManager.
    pub(crate) fn handle_heartbeat_ping(&mut self, payload: &str) {
        if let Ok(ping) = serde_json::from_str::<HeartbeatPing>(payload) {
            if let Some(hb_manager) = &self.heartbeat_manager {
                hb_manager.do_send(ProcesarPingHeartbeat { ping });
            }
        } else {
            warn!(
                "Nodo {}: Error parseando HEARTBEAT_PING: {}",
                self.id, payload
            );
        }
    }

    /// Procesa un mensaje HEARTBEAT_PONG delegándolo al HeartbeatManager.
    pub(crate) fn handle_heartbeat_pong(&mut self, payload: &str) {
        if let Ok(pong) = serde_json::from_str::<HeartbeatPong>(payload) {
            if let Some(hb_manager) = &self.heartbeat_manager {
                hb_manager.do_send(ProcesarPongHeartbeat { pong });
            }
        } else {
            warn!(
                "Nodo {}: Error parseando HEARTBEAT_PONG: {}",
                self.id, payload
            );
        }
    }

    /// Procesa un mensaje ELECTION del algoritmo Bully.
    ///
    /// Si el nodo emisor tiene ID menor, responde OK e inicia su propia elección.
    pub(crate) fn handle_election_message(&mut self, payload: &str, ctx: &mut Context<Self>) {
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(payload) {
            if let Some(from_id) = data["from"].as_u64() {
                let from_id = from_id as u32;
                if from_id < self.id {
                    if let Some(handler) = self.nodos_vecinos.get(&from_id) {
                        handler.do_send(EnviarMensaje {
                            tipo_mensaje: "OK".to_string(),
                            payload: serde_json::json!({"from": self.id}).to_string(),
                        });
                    }
                    if self.rol != RolNodo::Lider {
                        self.iniciar_eleccion(ctx);
                    }
                }
            }
        }
    }

    /// Procesa un mensaje OK indicando que hay un nodo con mayor prioridad.
    pub(crate) fn handle_ok_message(&mut self, payload: &str) {
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(payload) {
            if let Some(_from_id) = data["from"].as_u64() {
                self.en_eleccion = false;
            }
        }
    }

    /// Procesa un mensaje COORDINATOR anunciando el nuevo líder del cluster.
    pub(crate) fn handle_coordinator_message(&mut self, payload: &str) {
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(payload) {
            if let Some(leader_id) = data["leader_id"].as_u64() {
                let leader_id = leader_id as u32;

                self.id_lider = Some(leader_id);
                self.en_eleccion = false;

                if let Some(leader_addr) = data["leader_address"].as_str() {
                    self.address_lider = leader_addr.to_string();
                    info!(
                        "Nodo {}: Actualizada dirección del líder {} a {}",
                        self.id, leader_id, leader_addr
                    );
                }

                if leader_id != self.id {
                    if self.rol == RolNodo::Replica && !self.estado_global_replica.is_empty() {
                        info!(
                            "Nodo {}: Era RÉPLICA, transfiriendo estado global del líder anterior al nuevo líder {}",
                            self.id, leader_id
                        );

                        if let Some(handler_nuevo_lider) = self.nodos_vecinos.get(&leader_id) {
                            let estado_para_transferir = self.estado_global_replica.clone();
                            match serde_json::to_string(&estado_para_transferir) {
                                Ok(payload) => {
                                    info!(
                                        "Nodo {}: Enviando estado global ({} compañías) al nuevo líder {}",
                                        self.id, estado_para_transferir.len(), leader_id
                                    );

                                    handler_nuevo_lider.do_send(EnviarMensaje {
                                        tipo_mensaje: "ESTADO_TRANSFERIDO_REPLICA".to_string(),
                                        payload,
                                    });
                                }
                                Err(e) => {
                                    error!(
                                        "Nodo {}: Error serializando estado para transferir: {}",
                                        self.id, e
                                    );
                                }
                            }
                        } else {
                            warn!(
                                "Nodo {}: No se encontró handler para nuevo líder {} para transferir estado",
                                self.id, leader_id
                            );
                        }

                        self.estado_global_replica.clear();
                    }

                    self.rol = RolNodo::Ninguno;
                    self.estado_global.clear();
                }
            }
        }
    }

    /// Procesa un mensaje REPLICA convirtiéndose en réplica del líder.
    pub(crate) fn handle_replica_message(&mut self, payload: &str) {
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(payload) {
            info!("Mi lider es el siguiente: {:?}", data["leader_id"]);
            if let Some(_leader_id) = data["leader_id"].as_u64() {
                self.rol = RolNodo::Replica;
                info!("Nodo {}: Designado como RÉPLICA por el líder", self.id);
            }
        }
    }

    pub(crate) fn handle_estado_inicial(&mut self, payload: &str) {
        if self.rol != RolNodo::Replica {
            return;
        }
        if let Ok(estado) = serde_json::from_str::<HashMap<u32, EstadoCompania>>(payload) {
            self.estado_global_replica = estado;
            info!(
                "Nodo {} (Réplica): Recibido estado inicial ({} compañías)",
                self.id,
                self.estado_global_replica.len()
            );
        }
    }

    pub(crate) fn handle_estado_transferido_replica(&mut self, payload: &str) {
        if self.rol != RolNodo::Lider {
            warn!(
                "Nodo {}: Recibido ESTADO_TRANSFERIDO_REPLICA pero no soy líder",
                self.id
            );
            return;
        }

        if let Ok(estado_transferido) =
            serde_json::from_str::<HashMap<u32, EstadoCompania>>(payload)
        {
            info!(
                "Líder {}: Recibido estado transferido de réplica anterior ({} compañías)",
                self.id,
                estado_transferido.len()
            );

            if self.estado_global.is_empty() {
                self.estado_global = estado_transferido;
                info!(
                    "Líder {}: Estado global restaurado desde réplica anterior ({} compañías)",
                    self.id,
                    self.estado_global.len()
                );
            } else {
                for (compania_id, estado) in estado_transferido {
                    self.estado_global.insert(compania_id, estado);
                }
                info!(
                    "Líder {}: Estado global mergeado con estado de réplica anterior ({} compañías total)",
                    self.id, self.estado_global.len()
                );
            }

            if let Some(id_replica) = self.id_replica {
                if let Some(handler_replica) = self.nodos_vecinos.get(&id_replica) {
                    if let Ok(estado_serializado) = serde_json::to_string(&self.estado_global) {
                        handler_replica.do_send(EnviarMensaje {
                            tipo_mensaje: "ESTADO_INICIAL".to_string(),
                            payload: estado_serializado,
                        });
                        info!(
                            "Líder {}: Estado actualizado sincronizado con réplica {} ({} compañías)",
                            self.id,
                            id_replica,
                            self.estado_global.len()
                        );
                    }
                }
            }
        } else {
            error!(
                "Líder {}: Error parseando estado transferido de réplica",
                self.id
            );
        }
    }

    pub(crate) fn handle_who_is_leader(
        &self,
        _payload: &str,
        handler: Addr<CommunicationHandler<Self>>,
    ) {
        info!("Nodo {}: Recibida consulta WHO_IS_LEADER", self.id);

        let leader_address = self.address_lider.clone();

        let leader_info = common::LeaderInfo {
            leader_address: leader_address.clone(),
        };

        match serde_json::to_string(&leader_info) {
            Ok(payload) => {
                info!(
                    "Nodo {}: Respondiendo LEADER_INFO - Líder en {}",
                    self.id, leader_address
                );

                handler.do_send(EnviarMensaje {
                    tipo_mensaje: "LEADER_INFO".to_string(),
                    payload,
                });
            }
            Err(e) => {
                error!("Nodo {}: Error serializando LeaderInfo: {}", self.id, e);
            }
        }
    }

    pub(crate) fn handle_definir_limite_compania(&mut self, payload: &str) {
        if let Ok(limite_msg) = serde_json::from_str::<DefinirLimiteCompania>(payload) {
            info!(
                "Nodo {}: Definiendo límite de compañía {} a ${:.2}",
                self.id, limite_msg.compania_id, limite_msg.limite
            );

            let estado_map = if self.rol == RolNodo::Lider {
                &mut self.estado_global
            } else {
                &mut self.estado_global_replica
            };

            let estado_compania = estado_map.entry(limite_msg.compania_id).or_insert_with(|| {
                info!(
                    "Nodo {}: Creando nueva compañía {} con límite ${:.2}",
                    self.id, limite_msg.compania_id, limite_msg.limite
                );
                EstadoCompania::new(limite_msg.limite)
            });

            estado_compania.limite = limite_msg.limite;
            info!(
                "Nodo {}: Límite de compañía {} establecido en ${:.2}",
                self.id, limite_msg.compania_id, limite_msg.limite
            );

            if self.rol == RolNodo::Lider {
                if let Some(replica_id) = self.id_replica {
                    if let Some(replica_handler) = self.nodos_vecinos.get(&replica_id) {
                        info!(
                            "Líder {}: Sincronizando límite de compañía {} con réplica {}",
                            self.id, limite_msg.compania_id, replica_id
                        );
                        replica_handler.do_send(EnviarMensaje {
                            tipo_mensaje: "SYNC_LIMITE_COMPANIA".to_string(),
                            payload: payload.to_string(),
                        });
                    }
                }
            }
        }
    }

    pub(crate) fn handle_definir_limite_vehiculo(&mut self, payload: &str) {
        if let Ok(limite_msg) = serde_json::from_str::<DefinirLimiteVehiculo>(payload) {
            info!(
                "Nodo {}: Definiendo límite de vehículo {} en compañía {} a ${:.2}",
                self.id, limite_msg.vehiculo_id, limite_msg.compania_id, limite_msg.limite
            );

            let estado_map = if self.rol == RolNodo::Lider {
                &mut self.estado_global
            } else {
                &mut self.estado_global_replica
            };

            if let Some(estado_compania) = estado_map.get_mut(&limite_msg.compania_id) {
                estado_compania.agregar_vehiculo(limite_msg.vehiculo_id, limite_msg.limite);
                info!(
                    "Nodo {}: Límite de vehículo {} establecido en ${:.2}",
                    self.id, limite_msg.vehiculo_id, limite_msg.limite
                );

                if self.rol == RolNodo::Lider {
                    if let Some(replica_id) = self.id_replica {
                        if let Some(replica_handler) = self.nodos_vecinos.get(&replica_id) {
                            info!(
                                "Líder {}: Sincronizando límite de vehículo {} con réplica {}",
                                self.id, limite_msg.vehiculo_id, replica_id
                            );
                            replica_handler.do_send(EnviarMensaje {
                                tipo_mensaje: "SYNC_LIMITE_VEHICULO".to_string(),
                                payload: payload.to_string(),
                            });
                        }
                    }
                }
            } else {
                warn!(
                    "Nodo {}: No se puede definir límite de vehículo {} - compañía {} no tiene límite definido",
                    self.id, limite_msg.vehiculo_id, limite_msg.compania_id
                );
            }
        }
    }

    pub(crate) fn handle_consultar_saldo_vehiculo(
        &self,
        payload: &str,
        handler: Addr<CommunicationHandler<Self>>,
    ) {
        if self.rol != RolNodo::Lider {
            warn!(
                "Nodo {}: Solo el líder puede consultar saldos, ignorando",
                self.id
            );
            return;
        }

        match serde_json::from_str::<ConsultarSaldoVehiculo>(payload) {
            Ok(consulta) => {
                info!(
                    "Líder {}: Consultando saldo de vehículo {}",
                    self.id, consulta.vehiculo_id
                );

                let saldo = if self.estado_global.is_empty() {
                    warn!("Líder {}: No tengo estado global disponible", self.id);
                    0.0
                } else {
                    let mut resultado = 0.0;
                    for estado in self.estado_global.values() {
                        if let Some(vehiculo) = estado.vehiculos.get(&consulta.vehiculo_id) {
                            resultado = vehiculo.saldo_disponible();
                            break;
                        }
                    }
                    resultado
                };

                let respuesta = ResponderSaldo {
                    saldo,
                    tipo: TipoConsulta::Vehiculo(consulta.vehiculo_id),
                };

                if let Ok(payload) = serde_json::to_string(&respuesta) {
                    info!(
                        "Líder {}: Enviando saldo de vehículo {}: ${:.2}",
                        self.id, consulta.vehiculo_id, saldo
                    );

                    handler.do_send(EnviarMensaje {
                        tipo_mensaje: "RESPONDER_SALDO".to_string(),
                        payload,
                    });
                }
            }
            Err(e) => {
                error!(
                    "Nodo {}: Error deserializando ConsultarSaldoVehiculo: {}",
                    self.id, e
                );
            }
        }
    }

    pub(crate) fn handle_consultar_saldo_compania(
        &self,
        payload: &str,
        handler: Addr<CommunicationHandler<Self>>,
    ) {
        if self.rol != RolNodo::Lider {
            warn!(
                "Nodo {}: Solo el líder puede consultar saldos, ignorando",
                self.id
            );
            return;
        }

        match serde_json::from_str::<ConsultarSaldoCompania>(payload) {
            Ok(consulta) => {
                info!(
                    "Líder {}: Consultando saldo de compañía {}",
                    self.id, consulta.compania_id
                );

                let saldo = if self.estado_global.is_empty() {
                    warn!("Líder {}: No tengo estado global disponible", self.id);
                    0.0
                } else {
                    self.estado_global
                        .get(&consulta.compania_id)
                        .map(|estado| estado.saldo_disponible())
                        .unwrap_or(0.0)
                };

                let respuesta = ResponderSaldo {
                    saldo,
                    tipo: TipoConsulta::Compania(consulta.compania_id),
                };

                if let Ok(payload) = serde_json::to_string(&respuesta) {
                    info!(
                        "Líder {}: Enviando saldo de compañía {}: ${:.2}",
                        self.id, consulta.compania_id, saldo
                    );

                    handler.do_send(EnviarMensaje {
                        tipo_mensaje: "RESPONDER_SALDO".to_string(),
                        payload,
                    });
                }
            }
            Err(e) => {
                error!(
                    "Líder {}: Error deserializando ConsultarSaldoCompania: {}",
                    self.id, e
                );
            }
        }
    }

    pub(crate) fn handle_solicitar_reporte_mensual(
        &mut self,
        payload: &str,
        handler: Addr<CommunicationHandler<Self>>,
    ) {
        if self.rol != RolNodo::Lider {
            warn!(
                "Nodo {}: Solo el líder puede generar reportes, ignorando",
                self.id
            );
            return;
        }

        match serde_json::from_str::<SolicitarReporteMensual>(payload) {
            Ok(solicitud) => {
                info!(
                    "Líder {}: Generando reporte mensual para compañía {}",
                    self.id, solicitud.id_compania
                );

                let (consumo_total, consumo_por_vehiculo) =
                    if let Some(estado) = self.estado_global.get_mut(&solicitud.id_compania) {
                        let total = estado.consumo;
                        let por_vehiculo: Vec<(u32, f64)> = estado
                            .vehiculos
                            .iter()
                            .map(|(&id, vehiculo)| (id, vehiculo.consumo))
                            .collect();

                        estado.consumo = 0.0;
                        for vehiculo in estado.vehiculos.values_mut() {
                            vehiculo.consumo = 0.0;
                        }

                        info!(
                            "Líder {}: Consumos de compañía {} reseteados para nuevo periodo",
                            self.id, solicitud.id_compania
                        );

                        (total, por_vehiculo)
                    } else {
                        warn!(
                        "Líder {}: Compañía {} no encontrada o no tengo estado global disponible",
                        self.id, solicitud.id_compania
                    );
                        (0.0, Vec::new())
                    };

                let cantidad_vehiculos = consumo_por_vehiculo.len();
                let reporte = ReporteMensual {
                    consumo_total,
                    consumo_por_vehiculo,
                };

                if let Ok(payload) = serde_json::to_string(&reporte) {
                    info!("Líder {}: Enviando reporte mensual - Consumo total: ${:.2}, {} vehículos [RESETEADO]",
                          self.id, consumo_total, cantidad_vehiculos);

                    handler.do_send(EnviarMensaje {
                        tipo_mensaje: "REPORTE_MENSUAL".to_string(),
                        payload,
                    });
                }
            }
            Err(e) => {
                error!(
                    "Líder {}: Error deserializando SolicitarReporteMensual: {}",
                    self.id, e
                );
            }
        }
    }

    /// Procesa una nueva transacción desde una estación.
    ///
    /// Si es líder, procesa directamente. Si no, buferea para enviar al líder.
    pub(crate) fn handle_nueva_transaccion_estacion(
        &mut self,
        payload: &str,
        handler: Addr<CommunicationHandler<Self>>,
    ) {
        info!(
            "Nodo {}: Raw NUEVA_TRANSACCION_ESTACION payload: {}",
            self.id, payload
        );
        if let Ok(nueva_tx) = serde_json::from_str::<NuevaTransaccionEstacion>(payload) {
            let trans = nueva_tx.transaccion.clone();
            info!(
                "Nodo {}: Recibida TX {} de una estación - Vehículo {}",
                self.id, trans.id, trans.vehiculo_id
            );

            if self.rol == RolNodo::Lider {
                let aprobada;

                // Lógica de Idempotencia
                if self.almacen_idempotencia.esta_procesado(&trans.id) {
                    info!(
                        " idempotencia: TX {} ya fue procesada. Respondiendo OK.",
                        trans.id
                    );
                    aprobada = self
                        .almacen_idempotencia
                        .obtener_datos_transaccion(&trans.id)
                        .unwrap_or(false);
                } else {
                    // Lógica de Negocio
                    aprobada = self.procesar_transaccion(trans.clone());
                    if aprobada {
                        if let Some(replica_id) = self.id_replica {
                            if let Some(replica_handler) = self.nodos_vecinos.get(&replica_id) {
                                if let Some(estado_compania) =
                                    self.estado_global.get(&trans.compania_id)
                                {
                                    if let Some(estado_vehiculo) =
                                        estado_compania.vehiculos.get(&trans.vehiculo_id)
                                    {
                                        let actualizacion = common::ActualizacionEstado {
                                            compania_id: trans.compania_id,
                                            vehiculo_id: trans.vehiculo_id,
                                            consumo_compania: estado_compania.consumo,
                                            consumo_vehiculo: estado_vehiculo.consumo,
                                        };
                                        replica_handler.do_send(EnviarMensaje {
                                            tipo_mensaje: "SYNC_TRANSACCIONES".to_string(),
                                            payload: serde_json::to_string(&vec![actualizacion])
                                                .unwrap(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                    if let Err(e) = self
                        .almacen_idempotencia
                        .marcar_como_procesado(trans.id.clone(), aprobada)
                    {
                        error!(" Error al marcar TX {} como procesada: {}", trans.id, e);
                    }
                }

                let resultado = common::ResultadoTransaccionEstacion {
                    transaccion: trans.clone(),
                    aprobada,
                };

                if let Ok(payload) = serde_json::to_string(&resultado) {
                    handler.do_send(EnviarMensaje {
                        tipo_mensaje: "RESULTADO_TRANSACCION_ESTACION".to_string(),
                        payload,
                    });
                    info!(
                        "Nodo {}: Resultado enviado a estación para TX {} - Vehículo {} ({})",
                        self.id,
                        trans.id,
                        trans.vehiculo_id,
                        if aprobada { "APROBADA" } else { "RECHAZADA" }
                    );
                }
            } else if let Some(ref mut buffer) = self.buffer_transacciones {
                buffer.push(crate::cuenta_compania::TransaccionPendiente {
                    transaccion: trans.clone(),
                    manejador_respuesta: Some(handler.clone()),
                });
                if buffer.len() >= self.n_transacciones {
                    self.enviar_transacciones_al_lider();
                }
            }
        }
    }

    pub(crate) fn handle_transacciones_message(
        &mut self,
        payload: &str,
        handler: Addr<CommunicationHandler<Self>>,
    ) {
        if self.rol != RolNodo::Lider {
            return;
        }

        if let Ok(transacciones) = serde_json::from_str::<Vec<RegistroTransaccion>>(payload) {
            info!(
                "Nodo {} (Líder): Recibidas {} transacciones",
                self.id,
                transacciones.len()
            );

            let mut resultados = Vec::new(); // lo que devuelvo a la estación
            let mut transacciones_aceptadas = Vec::new(); // para sincronizar con la réplica

            for transaccion in &transacciones {
                let aceptada;
                // Lógica de Idempotencia
                if self.almacen_idempotencia.esta_procesado(&transaccion.id) {
                    info!(
                        " idempotencia: TX {} ya fue procesada. Respondiendo OK.",
                        transaccion.id
                    );
                    aceptada = self
                        .almacen_idempotencia
                        .obtener_datos_transaccion(&transaccion.id)
                        .unwrap_or(false);
                } else {
                    aceptada = self.procesar_transaccion(transaccion.clone());
                    if aceptada {
                        transacciones_aceptadas.push(transaccion.clone());
                    }
                }

                resultados.push(common::ResultadoTransaccionEstacion {
                    transaccion: transaccion.clone(),
                    aprobada: aceptada,
                });
            }

            if !transacciones_aceptadas.is_empty() {
                if let Some(replica_id) = self.id_replica {
                    if let Some(replica_handler) = self.nodos_vecinos.get(&replica_id) {
                        let mut actualizaciones: Vec<common::ActualizacionEstado> = Vec::new();

                        for trans in &transacciones_aceptadas {
                            if let Some(estado_compania) =
                                self.estado_global.get(&trans.compania_id)
                            {
                                if let Some(estado_vehiculo) =
                                    estado_compania.vehiculos.get(&trans.vehiculo_id)
                                {
                                    actualizaciones.push(common::ActualizacionEstado {
                                        compania_id: trans.compania_id,
                                        vehiculo_id: trans.vehiculo_id,
                                        consumo_compania: estado_compania.consumo,
                                        consumo_vehiculo: estado_vehiculo.consumo,
                                    });
                                }
                            }
                        }

                        if !actualizaciones.is_empty() {
                            replica_handler.do_send(EnviarMensaje {
                                tipo_mensaje: "SYNC_TRANSACCIONES".to_string(),
                                payload: serde_json::to_string(&actualizaciones).unwrap(),
                            });
                        }
                    }
                }
            }

            for resultado_trans in &resultados {
                if let Err(e) = self.almacen_idempotencia.marcar_como_procesado(
                    resultado_trans.transaccion.id.clone(),
                    resultado_trans.aprobada,
                ) {
                    error!(
                        " Error al marcar TX {} como procesada: {}",
                        resultado_trans.transaccion.id, e
                    );
                }
            }

            handler.do_send(EnviarMensaje {
                tipo_mensaje: "RESULTADOS_TRANSACCIONES".to_string(),
                payload: serde_json::to_string(&resultados).unwrap(),
            });
        }
    }

    pub(crate) fn handle_sync_transacciones(&mut self, payload: &str) {
        if self.rol != RolNodo::Replica {
            return;
        }

        if let Ok(actualizaciones) =
            serde_json::from_str::<Vec<common::ActualizacionEstado>>(payload)
        {
            info!(
                "Réplica {}: Sincronizando {} actualizaciones de estado del líder.",
                self.id,
                actualizaciones.len()
            );
            for actualizacion in &actualizaciones {
                let estado_compania = self
                    .estado_global_replica
                    .entry(actualizacion.compania_id)
                    .or_insert_with(|| EstadoCompania::new(0.0));

                let estado_vehiculo = estado_compania
                    .vehiculos
                    .entry(actualizacion.vehiculo_id)
                    .or_insert_with(|| common::EstadoVehiculo::new(0.0));

                estado_vehiculo.consumo = actualizacion.consumo_vehiculo;
                estado_compania.consumo = actualizacion.consumo_compania;

                info!(
                    "Réplica {}: Estado sincronizado - Vehículo {} de compañía {} (consumo compañía: {:.2}, consumo vehículo: {:.2})",
                    self.id,
                    actualizacion.vehiculo_id,
                    actualizacion.compania_id,
                    estado_compania.consumo,
                    estado_vehiculo.consumo
                );
            }
        }
    }

    pub(crate) fn handle_resultados_transacciones(&mut self, payload: &str) {
        if let Ok(resultados) =
            serde_json::from_str::<Vec<common::ResultadoTransaccionEstacion>>(payload)
        {
            info!(
                "Nodo {}: Recibidos resultados de {} transacciones del líder",
                self.id,
                resultados.len()
            );

            if let Some(buffer) = self.buffer_transacciones.take() {
                for (idx, resultado) in resultados.iter().enumerate() {
                    info!(
                        "  {} TX {} - Vehículo {} de compañía {}: ${:.2} - {}",
                        if resultado.aprobada {
                            "APROBADA"
                        } else {
                            "RECHAZADA"
                        },
                        resultado.transaccion.id,
                        resultado.transaccion.vehiculo_id,
                        resultado.transaccion.compania_id,
                        resultado.transaccion.monto,
                        if resultado.aprobada {
                            "APROBADA"
                        } else {
                            "RECHAZADA"
                        }
                    );

                    if idx < buffer.len() {
                        if let Some(ref manejador) = buffer[idx].manejador_respuesta {
                            if let Ok(payload) = serde_json::to_string(resultado) {
                                manejador.do_send(EnviarMensaje {
                                    tipo_mensaje: "RESULTADO_TRANSACCION_ESTACION".to_string(),
                                    payload,
                                });
                            }
                        }
                    }
                }
                self.buffer_transacciones = Some(Vec::new());
            } else {
                for resultado in resultados {
                    info!(
                        "  (Sin buffer) {} TX {} - {}",
                        if resultado.aprobada {
                            "APROBADA"
                        } else {
                            "RECHAZADA"
                        },
                        resultado.transaccion.id,
                        if resultado.aprobada {
                            "APROBADA"
                        } else {
                            "RECHAZADA"
                        }
                    );
                }
            }
        }
    }

    pub(crate) fn handle_sync_limite_compania(&mut self, payload: &str) {
        if let Ok(limite_msg) = serde_json::from_str::<DefinirLimiteCompania>(payload) {
            info!(
                "Nodo {} (Réplica): Sincronizando límite de compañía {} a ${:.2}",
                self.id, limite_msg.compania_id, limite_msg.limite
            );

            let estado_compania = self
                .estado_global_replica
                .entry(limite_msg.compania_id)
                .or_insert_with(|| {
                    info!(
                        "Nodo {} (Réplica): Creando nueva compañía {} con límite ${:.2}",
                        self.id, limite_msg.compania_id, limite_msg.limite
                    );
                    EstadoCompania::new(limite_msg.limite)
                });

            estado_compania.limite = limite_msg.limite;
            info!(
                "Nodo {} (Réplica): Límite de compañía {} sincronizado en ${:.2}",
                self.id, limite_msg.compania_id, limite_msg.limite
            );
        }
    }

    pub(crate) fn handle_sync_limite_vehiculo(&mut self, payload: &str) {
        if let Ok(limite_msg) = serde_json::from_str::<DefinirLimiteVehiculo>(payload) {
            info!(
                "Nodo {} (Réplica): Sincronizando límite de vehículo {} en compañía {} a ${:.2}",
                self.id, limite_msg.vehiculo_id, limite_msg.compania_id, limite_msg.limite
            );

            if let Some(estado_compania) =
                self.estado_global_replica.get_mut(&limite_msg.compania_id)
            {
                estado_compania.agregar_vehiculo(limite_msg.vehiculo_id, limite_msg.limite);
                info!(
                    "Nodo {} (Réplica): Límite de vehículo {} sincronizado en ${:.2}",
                    self.id, limite_msg.vehiculo_id, limite_msg.limite
                );
            } else {
                warn!(
                    "Nodo {} (Réplica): Compañía {} no existe para sincronizar límite del vehículo {}",
                    self.id, limite_msg.compania_id, limite_msg.vehiculo_id
                );
            }
        }
    }
}
