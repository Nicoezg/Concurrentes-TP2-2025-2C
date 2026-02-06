use crate::CommunicationHandler;
use actix::{Actor, Addr, Context, Handler};
use common::{ActorConnected, ErrorConexion, MensajeRecibido};
use log::{error, info};
use tokio::net::TcpListener;

/// Actor que acepta conexiones TCP entrantes.
///
/// Escucha en una dirección específica y crea un `CommunicationHandler` para cada
/// nueva conexión aceptada. Delega el manejo de mensajes al actor destino especificado.
pub struct Acceptor<T>
where
    T: Actor<Context = Context<T>>
        + Handler<MensajeRecibido<CommunicationHandler<T>>>
        + Handler<ErrorConexion<CommunicationHandler<T>>>
        + Handler<ActorConnected<CommunicationHandler<T>>>,
{
    /// Dirección IP:Puerto donde escuchar
    bind_addr: String,
    /// Actor que recibirá los mensajes de las conexiones aceptadas
    actor_destino: Addr<T>,
}

impl<T> Acceptor<T>
where
    T: Actor<Context = Context<T>>
        + Handler<MensajeRecibido<CommunicationHandler<T>>>
        + Handler<ErrorConexion<CommunicationHandler<T>>>
        + Handler<ActorConnected<CommunicationHandler<T>>>,
{
    /// Crea un nuevo acceptor que escuchará en la dirección especificada.
    pub fn new(bind_addr: String, actor_destino: Addr<T>) -> Self {
        Self {
            bind_addr,
            actor_destino,
        }
    }
}

impl<T> Actor for Acceptor<T>
where
    T: Actor<Context = Context<T>>
        + Handler<MensajeRecibido<CommunicationHandler<T>>>
        + Handler<ErrorConexion<CommunicationHandler<T>>>
        + Handler<ActorConnected<CommunicationHandler<T>>>
        + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        let bind_addr = self.bind_addr.clone();
        let actor_destino = self.actor_destino.clone();

        info!("Acceptor iniciando en {}", bind_addr);

        // Crear el listener TCP en background usando actix::spawn
        actix::spawn(async move {
            match TcpListener::bind(&bind_addr).await {
                Ok(listener) => {
                    info!("✓ Acceptor escuchando en {}", bind_addr);

                    loop {
                        match listener.accept().await {
                            Ok((stream, addr)) => {
                                info!("Nueva conexión desde {}", addr);

                                // Crear un CommunicationHandler para esta conexión
                                let _handler_addr = CommunicationHandler::new(
                                    stream,
                                    addr.to_string(),
                                    actor_destino.clone(),
                                    false, // acceptor nunca es initiator
                                )
                                .start();
                            }
                            Err(e) => {
                                error!("Error aceptando conexión: {}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Error vinculando a {}: {}", bind_addr, e);
                }
            }
        });
    }
}
