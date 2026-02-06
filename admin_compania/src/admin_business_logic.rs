use crate::admin_types::{AdministradorDeCompania, ComandoPendiente};
use actix::{AsyncContext, Context};
use common::{
    ConsultarSaldoCompania, ConsultarSaldoVehiculo, DefinirLimiteCompania, DefinirLimiteVehiculo,
    EnviarMensaje, SolicitarReporteMensual,
};
use log::{error, info, warn};

impl AdministradorDeCompania {
    /// Detecta falla del líder y limpia el estado
    pub(crate) fn detectar_falla_lider(&mut self, ctx: &mut Context<Self>) {
        warn!("Admin {}: Detectada falla del líder", self.compania_id);

        if let Some(comando) = self.ultimo_comando_enviado.take() {
            info!(
                "Admin {}: Reencolando último comando que falló",
                self.compania_id
            );
            self.comandos_pendientes.push(comando);
        }

        info!(
            "Admin {}: {} comandos pendientes. Reiniciando descubrimiento del líder...",
            self.compania_id,
            self.comandos_pendientes.len()
        );

        // NO sacar la información del lider actual para despues excluirlo

        // Reiniciar el proceso de descubrimiento del líder
        ctx.address().do_send(crate::admin_types::DescubrirLider);
    }

    pub fn definir_limite_compania(&mut self, limite: f64, _ctx: &mut Context<Self>) {
        let comando = ComandoPendiente::DefinirLimiteCompania { limite };

        if let Some(handler) = &self.handler_lider {
            let msg = DefinirLimiteCompania {
                compania_id: self.compania_id,
                limite,
            };

            match serde_json::to_string(&msg) {
                Ok(payload) => {
                    info!(
                        "Admin de compañía {}: Definiendo límite de compañía a ${:.2}",
                        self.compania_id, limite
                    );

                    // Guardar como último comando enviado por si falla
                    self.ultimo_comando_enviado = Some(comando);

                    handler.do_send(EnviarMensaje {
                        tipo_mensaje: "DEFINIR_LIMITE_COMPANIA".to_string(),
                        payload,
                    });
                }
                Err(e) => {
                    error!(
                        "Admin {}: Error serializando mensaje, encolando comando: {}",
                        self.compania_id, e
                    );
                    self.comandos_pendientes.push(comando);
                }
            }
        } else {
            info!(
                "Admin {}: Líder aún no disponible, encolando comando (líder: {:?})",
                self.compania_id, self.lider_actual
            );
            self.comandos_pendientes.push(comando);
        }
    }

    pub fn definir_limite_vehiculo(
        &mut self,
        vehiculo_id: u32,
        limite: f64,
        _ctx: &mut Context<Self>,
    ) {
        if let Some(handler) = &self.handler_lider {
            let msg = DefinirLimiteVehiculo {
                compania_id: self.compania_id,
                vehiculo_id,
                limite,
            };

            match serde_json::to_string(&msg) {
                Ok(payload) => {
                    info!(
                        "Admin de compañía {}: Definiendo límite de vehículo {} a ${:.2}",
                        self.compania_id, vehiculo_id, limite
                    );

                    handler.do_send(EnviarMensaje {
                        tipo_mensaje: "DEFINIR_LIMITE_VEHICULO".to_string(),
                        payload,
                    });
                }
                Err(e) => {
                    error!(
                        "Admin {}: Error serializando mensaje, encolando comando: {}",
                        self.compania_id, e
                    );
                    self.comandos_pendientes
                        .push(ComandoPendiente::DefinirLimiteVehiculo {
                            vehiculo_id,
                            limite,
                        });
                }
            }
        } else {
            info!(
                "Admin {}: Líder aún no disponible, encolando comando",
                self.compania_id
            );
            self.comandos_pendientes
                .push(ComandoPendiente::DefinirLimiteVehiculo {
                    vehiculo_id,
                    limite,
                });
        }
    }

    pub fn consultar_saldo_compania(&mut self, _ctx: &mut Context<Self>) {
        if let Some(handler) = &self.handler_lider {
            let msg = ConsultarSaldoCompania {
                compania_id: self.compania_id,
            };

            match serde_json::to_string(&msg) {
                Ok(payload) => {
                    info!(
                        "Admin de compañía {}: Consultando saldo de compañía",
                        self.compania_id
                    );

                    handler.do_send(EnviarMensaje {
                        tipo_mensaje: "CONSULTAR_SALDO_COMPANIA".to_string(),
                        payload,
                    });
                }
                Err(e) => {
                    error!(
                        "Admin {}: Error serializando mensaje, encolando comando: {}",
                        self.compania_id, e
                    );
                    self.comandos_pendientes
                        .push(ComandoPendiente::ConsultarSaldoCompania);
                }
            }
        } else {
            info!(
                "Admin {}: Líder aún no disponible, encolando comando",
                self.compania_id
            );
            self.comandos_pendientes
                .push(ComandoPendiente::ConsultarSaldoCompania);
        }
    }

    pub fn consultar_saldo_vehiculo(&mut self, vehiculo_id: u32, _ctx: &mut Context<Self>) {
        if let Some(handler) = &self.handler_lider {
            let msg = ConsultarSaldoVehiculo { vehiculo_id };

            match serde_json::to_string(&msg) {
                Ok(payload) => {
                    info!(
                        "Admin de compañía {}: Consultando saldo de vehículo {}",
                        self.compania_id, vehiculo_id
                    );

                    handler.do_send(EnviarMensaje {
                        tipo_mensaje: "CONSULTAR_SALDO_VEHICULO".to_string(),
                        payload,
                    });
                }
                Err(e) => {
                    error!(
                        "Admin {}: Error serializando mensaje, encolando comando: {}",
                        self.compania_id, e
                    );
                    self.comandos_pendientes
                        .push(ComandoPendiente::ConsultarSaldoVehiculo { vehiculo_id });
                }
            }
        } else {
            info!(
                "Admin {}: Líder aún no disponible, encolando comando",
                self.compania_id
            );
            self.comandos_pendientes
                .push(ComandoPendiente::ConsultarSaldoVehiculo { vehiculo_id });
        }
    }

    pub fn solicitar_reporte_mensual(&mut self, _ctx: &mut Context<Self>) {
        let comando = ComandoPendiente::SolicitarReporteMensual;

        if let Some(handler) = &self.handler_lider {
            let msg = SolicitarReporteMensual {
                id_compania: self.compania_id,
            };

            match serde_json::to_string(&msg) {
                Ok(payload) => {
                    info!(
                        "Admin de compañía {}: Solicitando reporte mensual",
                        self.compania_id
                    );

                    // Guardar como último comando enviado por si falla
                    self.ultimo_comando_enviado = Some(comando.clone());

                    handler.do_send(EnviarMensaje {
                        tipo_mensaje: "SOLICITAR_REPORTE_MENSUAL".to_string(),
                        payload,
                    });
                }
                Err(e) => {
                    error!(
                        "Admin {}: Error serializando mensaje, encolando comando: {}",
                        self.compania_id, e
                    );
                    self.comandos_pendientes.push(comando);
                }
            }
        } else {
            info!(
                "Admin {}: Líder aún no disponible, encolando comando",
                self.compania_id
            );
            self.comandos_pendientes.push(comando);
        }
    }

    /// Procesa todos los comandos pendientes una vez establecida la conexión con el líder
    pub(crate) fn procesar_comandos_pendientes(&mut self, ctx: &mut Context<Self>) {
        if self.comandos_pendientes.is_empty() {
            return;
        }

        info!(
            "Admin {}: Procesando {} comandos pendientes",
            self.compania_id,
            self.comandos_pendientes.len()
        );

        let comandos = std::mem::take(&mut self.comandos_pendientes);
        for comando in comandos {
            match comando {
                ComandoPendiente::DefinirLimiteCompania { limite } => {
                    self.definir_limite_compania(limite, ctx);
                }
                ComandoPendiente::DefinirLimiteVehiculo {
                    vehiculo_id,
                    limite,
                } => {
                    self.definir_limite_vehiculo(vehiculo_id, limite, ctx);
                }
                ComandoPendiente::ConsultarSaldoCompania => {
                    self.consultar_saldo_compania(ctx);
                }
                ComandoPendiente::ConsultarSaldoVehiculo { vehiculo_id } => {
                    self.consultar_saldo_vehiculo(vehiculo_id, ctx);
                }
                ComandoPendiente::SolicitarReporteMensual => {
                    self.solicitar_reporte_mensual(ctx);
                }
            }
        }
    }
}
