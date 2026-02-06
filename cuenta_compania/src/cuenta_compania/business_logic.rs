//! Lógica de negocio del nodo de cuenta de compañía.
//!
//! Implementa el algoritmo de elección de líder (Bully), promoción de rol,
//! selección de réplica y procesamiento de transacciones.

use crate::cuenta_compania::CuentaCompania;
use actix::{AsyncContext, Context};
use common::{EnviarMensaje, RegistroTransaccion, RolNodo};
use log::{debug, info, warn};
use std::time::Duration;

impl CuentaCompania {
    /// Inicia el algoritmo de elección de líder (Bully).
    ///
    /// Envía mensajes ELECTION a todos los nodos con ID mayor. Si no hay respuesta
    /// en 2 segundos, se convierte en líder.
    pub(crate) fn iniciar_eleccion(&mut self, ctx: &mut Context<Self>) {
        if self.en_eleccion {
            debug!("Nodo {}: Elección ya en curso, ignorando", self.id);
            return;
        }
        if self.rol == RolNodo::Lider {
            debug!(
                "Nodo {}: Ya soy líder, ignorando solicitud de elección",
                self.id
            );
            return;
        }

        info!("Nodo {}: Iniciando elección de líder", self.id);
        self.en_eleccion = true;

        let nodos_mayores: Vec<u32> = self
            .nodos_vecinos
            .keys()
            .filter(|&&id| id > self.id)
            .copied()
            .collect();

        if nodos_mayores.is_empty() {
            info!(
                "Nodo {}: No hay nodos con ID mayor, me convierto en líder",
                self.id
            );
            self.convertirse_en_lider(ctx);
        } else {
            info!(
                "Nodo {}: Enviando ELECTION a nodos: {:?}",
                self.id, nodos_mayores
            );
            for nodo_id in nodos_mayores {
                if let Some(handler) = self.nodos_vecinos.get(&nodo_id) {
                    handler.do_send(EnviarMensaje {
                        tipo_mensaje: "ELECTION".to_string(),
                        payload: (serde_json::json!({"from": self.id}).to_string()),
                    });
                }
            }

            let mi_id = self.id;
            ctx.run_later(Duration::from_secs(2), move |act, ctx| {
                if act.en_eleccion && act.id_lider.is_none() {
                    info!("Nodo {}: Timeout en elección, me convierto en líder", mi_id);
                    act.convertirse_en_lider(ctx);
                }
            });
        }
    }

    /// Convierte este nodo en líder del cluster.
    ///
    /// Si era réplica, restaura el estado global desde la copia local.
    /// Anuncia su liderazgo a todos los nodos y selecciona una nueva réplica.
    pub(crate) fn convertirse_en_lider(&mut self, _ctx: &mut Context<Self>) {
        info!("Nodo {}: Convirtiéndose en LÍDER", self.id);
        info!("Mi rol era: {:?}", self.rol);

        if self.rol == RolNodo::Replica {
            info!(
                "Mi estado global de replica era el siguiente: {:?}",
                self.estado_global_replica
            );
            self.estado_global = self.estado_global_replica.clone();
            self.estado_global_replica.clear();

            info!(
                "Nodo {}: Promovido de RÉPLICA a LÍDER, estado global restaurado ({} compañías)",
                self.id,
                self.estado_global.len()
            );
        }

        self.rol = RolNodo::Lider;
        self.id_lider = Some(self.id);
        self.en_eleccion = false;
        self.address_lider = format!("127.0.0.1:{}", 9000 + self.id);

        if let Some(buffer) = self.buffer_transacciones.take() {
            for tx_pendiente in buffer {
                let aprobada = self.procesar_transaccion(tx_pendiente.transaccion.clone());

                if let Some(handler) = tx_pendiente.manejador_respuesta {
                    let resultado = common::ResultadoTransaccionEstacion {
                        transaccion: tx_pendiente.transaccion.clone(),
                        aprobada,
                    };

                    if let Ok(payload) = serde_json::to_string(&resultado) {
                        handler.do_send(EnviarMensaje {
                            tipo_mensaje: "RESULTADO_TRANSACCION_ESTACION".to_string(),
                            payload,
                        });
                        info!(
                            "Nodo {}: Resultado de buffer enviado a estación - Vehículo {} ({})",
                            self.id,
                            tx_pendiente.transaccion.vehiculo_id,
                            if aprobada { "APROBADA" } else { "RECHAZADA" }
                        );
                    }
                }
            }
        }
        self.buffer_transacciones = None;

        for handler in self.nodos_vecinos.values() {
            handler.do_send(EnviarMensaje {
                tipo_mensaje: "COORDINATOR".to_string(),
                payload: (serde_json::json!({
                    "leader_id": self.id,
                    "leader_address": self.address_lider.clone()
                })
                .to_string()),
            });
        }

        if let Some(&replica_id) = self.nodos_vecinos.keys().max() {
            info!(
                "Nodo {}: Eligiendo a nodo {} como réplica",
                self.id, replica_id
            );
            self.id_replica = Some(replica_id);

            if let Some(handler) = self.nodos_vecinos.get(&replica_id) {
                handler.do_send(EnviarMensaje {
                    tipo_mensaje: "REPLICA".to_string(),
                    payload: serde_json::json!({"leader_id": self.id}).to_string(),
                });

                handler.do_send(EnviarMensaje {
                    tipo_mensaje: "ESTADO_INICIAL".to_string(),
                    payload: serde_json::to_string(&self.estado_global).unwrap(),
                });
            }
        }
    }

    /// Procesa una transacción validando límites de compañía y vehículo.
    ///
    /// Retorna true si la transacción fue aprobada y el estado actualizado,
    /// false si fue rechazada por exceder algún límite.
    pub(crate) fn procesar_transaccion(&mut self, transaccion: RegistroTransaccion) -> bool {
        let compania_id = transaccion.compania_id;
        let vehiculo_id = transaccion.vehiculo_id;
        let monto = transaccion.monto;

        let estado_compania = match self.estado_global.get_mut(&compania_id) {
            Some(estado) => estado,
            None => {
                warn!(
                    "Nodo {} (Líder): Transacción RECHAZADA - Compañía {} no tiene límite definido",
                    self.id, compania_id
                );
                return false;
            }
        };

        if estado_compania.consumo + monto > estado_compania.limite {
            warn!(
                "Nodo {} (Líder): Transacción RECHAZADA - Compañía {} excede límite (consumo: {:.2}, límite: {:.2}, intento: {:.2})",
                self.id, compania_id, estado_compania.consumo, estado_compania.limite, monto
            );
            return false;
        }

        let limite_vehiculo = estado_compania
            .vehiculos
            .get(&vehiculo_id)
            .map(|v| v.limite)
            .unwrap_or(estado_compania.limite);

        let estado_vehiculo = estado_compania
            .vehiculos
            .entry(vehiculo_id)
            .or_insert_with(|| common::EstadoVehiculo::new(limite_vehiculo));

        if estado_vehiculo.consumo + monto > estado_vehiculo.limite {
            warn!(
                "Nodo {} (Líder): Transacción RECHAZADA - Vehículo {} de compañía {} excede límite (consumo: {:.2}, límite: {:.2}, intento: {:.2})",
                self.id, vehiculo_id, compania_id, estado_vehiculo.consumo, estado_vehiculo.limite, monto
            );
            return false;
        }

        estado_vehiculo.consumo += monto;
        estado_compania.consumo += monto;

        info!(
            "Nodo {} (Líder): Transacción ACEPTADA - Vehículo {} de compañía {} cargó ${:.2} (saldo compañía: {:.2}/{:.2}, saldo vehículo: {:.2}/{:.2})",
            self.id,
            vehiculo_id,
            compania_id,
            monto,
            estado_compania.consumo,
            estado_compania.limite,
            estado_vehiculo.consumo,
            estado_vehiculo.limite
        );
        true
    }

    pub(crate) fn enviar_transacciones_al_lider(&mut self) {
        if let Some(lider_id) = self.id_lider {
            if let Some(handler) = self.nodos_vecinos.get(&lider_id) {
                if let Some(buffer) = self.buffer_transacciones.take() {
                    info!(
                        "Nodo {}: Enviando {} transacciones al líder {}",
                        self.id,
                        buffer.len(),
                        lider_id
                    );

                    let transacciones: Vec<RegistroTransaccion> =
                        buffer.iter().map(|tp| tp.transaccion.clone()).collect();

                    handler.do_send(EnviarMensaje {
                        tipo_mensaje: "TRANSACCIONES".to_string(),
                        payload: serde_json::to_string(&transacciones).unwrap(),
                    });

                    self.buffer_transacciones = Some(buffer);
                }
            }
        }
    }
}
