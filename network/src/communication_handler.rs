use actix::{Actor, ActorContext, Addr, AsyncContext, Context, Handler};
use common::{
    ActorConnected, ActorType, CerrarConexion, EnviarMensaje, ErrorConexion, MensajeRecibido,
};
use log::{error, info, warn};
use tokio::io::{split, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// Información del actor transmitida al inicio de la conexión.
#[derive(serde::Serialize, serde::Deserialize)]
struct ActorInfo {
    actor_type: ActorType,
    id: u32,
}

/// Envía el tipo de actor e ID al inicio de la conexión.
async fn enviar_tipo_actor(
    writer: &mut tokio::io::WriteHalf<TcpStream>,
    tipo_actor: ActorType,
    id: u32,
    direccion: &str,
) -> Result<(), String> {
    let info = ActorInfo {
        actor_type: tipo_actor,
        id,
    };
    let tipo_json = serde_json::to_string(&info).map_err(|e| e.to_string())?;
    let msg = format!("ACTOR_TYPE:{}\n", tipo_json);

    writer
        .write_all(msg.as_bytes())
        .await
        .map_err(|e| format!("Error enviando tipo de actor: {}", e))?;

    writer
        .flush()
        .await
        .map_err(|e| format!("Error flushing: {}", e))?;

    info!("✓ Tipo de actor enviado a {}", direccion);
    Ok(())
}

/// Envía un mensaje a través de la conexión TCP.
async fn enviar_mensaje_tcp(
    writer: &mut tokio::io::WriteHalf<TcpStream>,
    mensaje: &str,
) -> Result<(), String> {
    writer
        .write_all(mensaje.as_bytes())
        .await
        .map_err(|e| format!("Error escribiendo: {}", e))?;

    writer
        .write_all(b"\n")
        .await
        .map_err(|e| format!("Error escribiendo newline: {}", e))?;

    writer
        .flush()
        .await
        .map_err(|e| format!("Error flushing: {}", e))?;

    Ok(())
}

/// Procesa un mensaje recibido y lo despacha al actor correspondiente.
fn procesar_mensaje_recibido<T>(
    mensaje: &str,
    esperando_tipo: &mut bool,
    direccion_remota: &str,
    actor_destino: &Addr<T>,
    handler_addr: &Addr<CommunicationHandler<T>>,
) where
    T: Actor<Context = Context<T>>
        + Handler<MensajeRecibido<CommunicationHandler<T>>>
        + Handler<ErrorConexion<CommunicationHandler<T>>>
        + Handler<ActorConnected<CommunicationHandler<T>>>
        + 'static,
{
    if let Some((tipo, payload)) = mensaje.split_once(':') {
        if *esperando_tipo && tipo == "ACTOR_TYPE" {
            manejar_actor_type(payload, direccion_remota, actor_destino, handler_addr);
            *esperando_tipo = false;
        } else {
            info!("← Recibido {} de {}", tipo, direccion_remota);
            actor_destino.do_send(MensajeRecibido {
                tipo_mensaje: tipo.to_string(),
                payload: payload.to_string(),
                direccion_remota: direccion_remota.to_string(),
                direccion: handler_addr.clone(),
            });
        }
    } else {
        warn!(
            "Mensaje mal formado desde {}: {}",
            direccion_remota, mensaje
        );
    }
}

/// Maneja el mensaje ACTOR_TYPE
fn manejar_actor_type<T>(
    payload: &str,
    direccion_remota: &str,
    actor_destino: &Addr<T>,
    handler_addr: &Addr<CommunicationHandler<T>>,
) where
    T: Actor<Context = Context<T>>
        + Handler<MensajeRecibido<CommunicationHandler<T>>>
        + Handler<ErrorConexion<CommunicationHandler<T>>>
        + Handler<ActorConnected<CommunicationHandler<T>>>
        + 'static,
{
    match serde_json::from_str::<ActorInfo>(payload) {
        Ok(info) => {
            info!(
                "✓ Tipo de actor recibido de {}: {:?} (id: {})",
                direccion_remota, info.actor_type, info.id
            );

            actor_destino.do_send(ActorConnected {
                actor_type: info.actor_type,
                direccion_remota: direccion_remota.to_string(),
                id: info.id,
                handler: handler_addr.clone(),
            });
        }
        Err(e) => {
            error!(
                "Error parseando tipo de actor de {}: {}",
                direccion_remota, e
            );
        }
    }
}

/// Actor que gestiona la comunicación bidireccional con un proceso remoto via TCP.
///
/// Maneja la lectura y escritura asíncrona de mensajes, el intercambio inicial de
/// información del actor, y la detección de errores de conexión.
pub struct CommunicationHandler<T>
where
    T: Actor<Context = Context<T>>
        + Handler<MensajeRecibido<Self>>
        + Handler<ErrorConexion<Self>>
        + Handler<ActorConnected<Self>>,
{
    /// Dirección IP:Puerto del proceso remoto
    direccion_remota: String,
    /// Actor que recibirá los mensajes de esta conexión
    actor_destino: Addr<T>,
    /// Stream TCP (tomado al iniciar)
    stream: Option<TcpStream>,
    /// Tipo y ID del actor remoto
    tipo_actor_id: Option<(ActorType, u32)>,
    /// Indica si este handler inició la conexión
    initiator: bool,
    /// Canal para enviar mensajes al escritor
    tx: Option<mpsc::UnboundedSender<String>>,
}

impl<T> CommunicationHandler<T>
where
    T: Actor<Context = Context<T>>
        + Handler<MensajeRecibido<Self>>
        + Handler<ErrorConexion<Self>>
        + Handler<ActorConnected<Self>>
        + 'static,
{
    /// Crea un nuevo handler para una conexión aceptada.
    pub fn new(
        stream: TcpStream,
        direccion_remota: String,
        actor_destino: Addr<T>,
        initiator: bool,
    ) -> Self {
        Self {
            direccion_remota,
            actor_destino,
            stream: Some(stream),
            tipo_actor_id: None,
            initiator,
            tx: None,
        }
    }

    /// Crea un nuevo handler para una conexión iniciada localmente.
    ///
    /// Este constructor incluye el tipo y ID del actor local para enviarlo al remoto.
    pub fn new_initiator(
        stream: TcpStream,
        direccion_remota: String,
        actor_destino: Addr<T>,
        tipo_actor: ActorType,
        id: u32,
    ) -> Self {
        Self {
            direccion_remota,
            actor_destino,
            stream: Some(stream),
            tipo_actor_id: Some((tipo_actor, id)),
            initiator: true,
            tx: None,
        }
    }

    /// Inicia el task para escribir mensajes al TCP
    fn spawn_writer_task(
        &self,
        mut writer: tokio::io::WriteHalf<TcpStream>,
        mut rx: mpsc::UnboundedReceiver<String>,
        handler_addr: Addr<Self>,
    ) {
        let direccion = self.direccion_remota.clone();
        let initiator = self.initiator;
        let tipo_actor_id = self.tipo_actor_id;
        let actor_destino = self.actor_destino.clone();

        tokio::spawn(async move {
            // Si somos initiator, enviamos nuestro tipo e ID primero
            if initiator {
                if let Some((tipo_actor, id)) = tipo_actor_id {
                    if let Err(e) = enviar_tipo_actor(&mut writer, tipo_actor, id, &direccion).await
                    {
                        error!("{}", e);
                        return;
                    }
                }
            }

            // Loop de escritura de mensajes
            while let Some(msg) = rx.recv().await {
                if let Err(e) = enviar_mensaje_tcp(&mut writer, &msg).await {
                    error!("Error escribiendo a {}: {}", direccion, e);
                    actor_destino.do_send(ErrorConexion {
                        descripcion: e,
                        handler: handler_addr.clone(),
                    });
                    break;
                }
            }
        });
    }

    /// Inicia el task para leer mensajes del TCP
    fn spawn_reader_task(
        &self,
        mut reader: tokio::io::ReadHalf<TcpStream>,
        tx: mpsc::UnboundedSender<String>,
        handler_addr: Addr<Self>,
    ) {
        let actor_destino = self.actor_destino.clone();
        let direccion = self.direccion_remota.clone();
        let mut esperando_tipo = !self.initiator;

        tokio::spawn(async move {
            let mut buffer = Vec::new();
            let mut temp_buf = [0u8; 1024];

            loop {
                match reader.read(&mut temp_buf).await {
                    Ok(0) => {
                        info!("Conexión cerrada por {}", direccion);
                        drop(tx); // Cerrar el canal interno
                        actor_destino.do_send(ErrorConexion {
                            descripcion: "Conexión cerrada por el remoto".to_string(),
                            handler: handler_addr.clone(),
                        });
                        break;
                    }
                    Ok(n) => {
                        buffer.extend_from_slice(&temp_buf[..n]);

                        // Procesar mensajes completos (terminados en \n)
                        while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
                            let line_bytes = buffer.drain(..=pos).collect::<Vec<u8>>();
                            let msg = String::from_utf8_lossy(&line_bytes[..line_bytes.len() - 1]);
                            let msg = msg.trim();

                            if !msg.is_empty() {
                                procesar_mensaje_recibido(
                                    msg,
                                    &mut esperando_tipo,
                                    &direccion,
                                    &actor_destino,
                                    &handler_addr,
                                );
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error leyendo de {}: {}", direccion, e);
                        drop(tx); // Cerrar el canal interno
                        actor_destino.do_send(ErrorConexion {
                            descripcion: e.to_string(),
                            handler: handler_addr.clone(),
                        });
                        break;
                    }
                }
            }

            // Detener el handler cuando termine el loop
            handler_addr.do_send(CerrarConexion {
                motivo: "Conexión terminada".to_string(),
            });
        });
    }
}

impl<T> Actor for CommunicationHandler<T>
where
    T: Actor<Context = Context<T>>
        + Handler<MensajeRecibido<Self>>
        + Handler<ErrorConexion<Self>>
        + Handler<ActorConnected<Self>>
        + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        info!(
            "✓ CommunicationHandler conectado con {}",
            self.direccion_remota
        );

        // Tomar el stream (solo se ejecuta una vez)
        let stream = match self.stream.take() {
            Some(s) => s,
            None => return,
        };

        let (tx, rx) = mpsc::unbounded_channel::<String>();
        let (reader, writer) = split(stream);

        // Guardar tx para enviar mensajes
        self.tx = Some(tx.clone());

        let handler_addr = ctx.address();

        // Iniciar task de escritura
        self.spawn_writer_task(writer, rx, handler_addr.clone());

        // Iniciar task de lectura
        self.spawn_reader_task(reader, tx, handler_addr);
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        info!(
            "✗ CommunicationHandler desconectado de {}",
            self.direccion_remota
        );
        // Asegurar que el canal interno se cierre
        self.tx = None;
    }
}

impl<T> Handler<EnviarMensaje> for CommunicationHandler<T>
where
    T: Actor<Context = Context<T>>
        + Handler<MensajeRecibido<Self>>
        + Handler<ErrorConexion<Self>>
        + Handler<ActorConnected<Self>>
        + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: EnviarMensaje, _ctx: &mut Self::Context) -> Self::Result {
        // Formato: TIPO:PAYLOAD
        let line = format!("{}:{}", msg.tipo_mensaje, msg.payload);
        info!(
            "→ Enviando {} a {}",
            msg.tipo_mensaje, self.direccion_remota
        );
        if let Some(ref tx) = self.tx {
            if let Err(e) = tx.send(line) {
                error!("Error enviando mensaje a canal: {}", e);
            }
        }
    }
}

impl<T> Handler<CerrarConexion> for CommunicationHandler<T>
where
    T: Actor<Context = Context<T>>
        + Handler<MensajeRecibido<Self>>
        + Handler<ErrorConexion<Self>>
        + Handler<ActorConnected<Self>>
        + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: CerrarConexion, ctx: &mut Self::Context) -> Self::Result {
        info!(
            "Cerrando conexión con {}: {}",
            self.direccion_remota, msg.motivo
        );
        ctx.stop();
    }
}
