use crate::admin_types::{
    AdministradorDeCompania, ComandoConsultarSaldoCompania, ComandoConsultarSaldoVehiculo,
    ComandoDefinirLimiteCompania, ComandoDefinirLimiteVehiculo, ComandoSolicitarReporteMensual,
    ConectarLider, DescubrirLider, EstablecerHandlerLider,
};
use actix::{Actor, AsyncContext, Context, Handler};
use common::{ActorConnected, EnviarMensaje, ErrorConexion};
use log::{error, info};

impl Actor for AdministradorDeCompania {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!("AdministradorDeCompania {} iniciado", self.compania_id);
        info!(
            "Admin {}: Iniciando conexión autónoma al líder...",
            self.compania_id
        );

        // Iniciar conexión al líder
        ctx.address().do_send(DescubrirLider);
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        info!("AdministradorDeCompania {} detenido", self.compania_id);
    }
}

// Handlers para los comandos del CLI
impl Handler<ComandoDefinirLimiteCompania> for AdministradorDeCompania {
    type Result = ();

    fn handle(
        &mut self,
        msg: ComandoDefinirLimiteCompania,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        self.definir_limite_compania(msg.limite, ctx);
    }
}

impl Handler<ComandoDefinirLimiteVehiculo> for AdministradorDeCompania {
    type Result = ();

    fn handle(
        &mut self,
        msg: ComandoDefinirLimiteVehiculo,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        self.definir_limite_vehiculo(msg.vehiculo_id, msg.limite, ctx);
    }
}

impl Handler<ComandoConsultarSaldoCompania> for AdministradorDeCompania {
    type Result = ();

    fn handle(
        &mut self,
        _msg: ComandoConsultarSaldoCompania,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        self.consultar_saldo_compania(ctx);
    }
}

impl Handler<ComandoConsultarSaldoVehiculo> for AdministradorDeCompania {
    type Result = ();

    fn handle(
        &mut self,
        msg: ComandoConsultarSaldoVehiculo,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        self.consultar_saldo_vehiculo(msg.vehiculo_id, ctx);
    }
}

impl Handler<ComandoSolicitarReporteMensual> for AdministradorDeCompania {
    type Result = ();

    fn handle(
        &mut self,
        _msg: ComandoSolicitarReporteMensual,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        self.solicitar_reporte_mensual(ctx);
    }
}

impl Handler<ErrorConexion<network::CommunicationHandler<AdministradorDeCompania>>>
    for AdministradorDeCompania
{
    type Result = ();

    fn handle(
        &mut self,
        msg: ErrorConexion<network::CommunicationHandler<AdministradorDeCompania>>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        error!(
            "Admin {}: Error de conexión: {}",
            self.compania_id, msg.descripcion
        );
        // Si tenemos un líder conectado y hay un error, es una falla del líder
        self.detectar_falla_lider(ctx);
    }
}

impl Handler<DescubrirLider> for AdministradorDeCompania {
    type Result = ();

    fn handle(&mut self, _msg: DescubrirLider, ctx: &mut Self::Context) -> Self::Result {
        if self.cluster_nodes.is_empty() {
            error!(
                "Admin {}: No hay nodos en el cluster configurados",
                self.compania_id
            );
            return;
        }

        // Excluir el nodo fallido si lo hay
        let nodos_disponibles: Vec<_> = if let Some(ref lider_fallido) = self.lider_actual {
            info!(
                "Admin {}: Excluyendo nodo fallido {} del descubrimiento",
                self.compania_id, lider_fallido
            );
            self.cluster_nodes
                .iter()
                .filter(|node| &node.address != lider_fallido)
                .cloned()
                .collect()
        } else {
            self.cluster_nodes.clone()
        };

        self.handler_lider = None;
        self.lider_actual = None;

        if nodos_disponibles.is_empty() {
            error!(
                "Admin {}: No hay nodos disponibles después de excluir el fallido",
                self.compania_id
            );
            return;
        }

        // Seleccionar un nodo aleatorio del cluster (excluyendo el fallido)
        let random_index = rand::random::<usize>() % nodos_disponibles.len();
        let node = nodos_disponibles[random_index].clone();

        info!(
            "Admin {}: Conectando a nodo aleatorio {} en {}...",
            self.compania_id, node.id, node.address
        );

        let compania_id = self.compania_id;
        let self_addr = ctx.address();

        let future = async move {
            match tokio::net::TcpStream::connect(&node.address).await {
                Ok(stream) => {
                    info!(
                        "Admin {}: Conectado a nodo {} - preguntando quién es el líder",
                        compania_id, node.id
                    );

                    let handler = network::CommunicationHandler::new_initiator(
                        stream,
                        node.address.clone(),
                        self_addr.clone(),
                        common::ActorType::AdminCompania,
                        compania_id,
                    )
                    .start();

                    // Enviar WHO_IS_LEADER al nodo
                    let who_is_leader = common::WhoIsLeader {
                        requester_id: compania_id,
                    };

                    if let Ok(payload) = serde_json::to_string(&who_is_leader) {
                        handler.do_send(EnviarMensaje {
                            tipo_mensaje: "WHO_IS_LEADER".to_string(),
                            payload,
                        });
                    }
                }
                Err(e) => {
                    error!(
                        "Admin {}: Error conectando a nodo {}: {}",
                        compania_id, node.id, e
                    );
                }
            }
        };

        ctx.spawn(actix::fut::wrap_future(future));
    }
}

impl Handler<ConectarLider> for AdministradorDeCompania {
    type Result = ();

    fn handle(&mut self, msg: ConectarLider, ctx: &mut Self::Context) -> Self::Result {
        info!(
            "Admin {}: Conectando al líder en {}...",
            self.compania_id, msg.leader_address
        );

        let compania_id = self.compania_id;
        let self_addr = ctx.address();
        let leader_address = msg.leader_address.clone();
        self.lider_actual = Some(leader_address.clone());

        let future = async move {
            match tokio::net::TcpStream::connect(&leader_address).await {
                Ok(stream) => {
                    info!(
                        "Admin {}: Conexión establecida con líder en {}",
                        compania_id, leader_address
                    );

                    // Crear el handler de comunicación con el líder
                    let handler = network::CommunicationHandler::new_initiator(
                        stream,
                        leader_address.clone(),
                        self_addr.clone(),
                        common::ActorType::AdminCompania,
                        compania_id,
                    )
                    .start();

                    // Enviar mensaje interno para establecer el handler del líder
                    self_addr.do_send(EstablecerHandlerLider {
                        handler,
                        leader_address: leader_address.clone(),
                    });
                }
                Err(e) => {
                    error!(
                        "Admin {}: Error conectando al líder en {}: {}",
                        compania_id, leader_address, e
                    );
                }
            }
        };

        ctx.spawn(actix::fut::wrap_future(future));
    }
}

impl Handler<EstablecerHandlerLider> for AdministradorDeCompania {
    type Result = ();

    fn handle(&mut self, msg: EstablecerHandlerLider, ctx: &mut Self::Context) -> Self::Result {
        info!(
            "Admin {}: Estableciendo handler del líder en {}",
            self.compania_id, msg.leader_address
        );

        // Guardar el handler del líder
        self.handler_lider = Some(msg.handler);

        info!(
            "Admin {}: Handler del líder establecido. Procesando comandos pendientes.",
            self.compania_id
        );

        // Procesar todos los comandos pendientes
        self.procesar_comandos_pendientes(ctx);
    }
}

impl Handler<ActorConnected<network::CommunicationHandler<Self>>> for AdministradorDeCompania {
    type Result = ();

    fn handle(
        &mut self,
        msg: ActorConnected<network::CommunicationHandler<Self>>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        info!(
            "Conexión inesperada recibida en Admin {} desde {}",
            self.compania_id, msg.direccion_remota
        );
    }
}
