//! Actor GPS que proporciona direcciones de estaciones cercanas a vehículos.

use actix::{Actor, Context, Handler};
use common::{
    ActorConnected, ActorType, Coordenadas, EnviarMensaje, ErrorConexion, MensajeRecibido,
    RespuestaDireccionEstacion, SolicitarDireccionEstacion,
};
use log::{error, info, warn};
use network::CommunicationHandler;

/// Radio de búsqueda en kilómetros para encontrar estaciones cercanas.
const RADIO_KM: f64 = 10.0;

/// Información de una estación de servicio.
#[derive(Debug, Clone)]
pub struct InfoEstacion {
    /// Ubicación geográfica de la estación
    pub coordenadas: Coordenadas,
    /// Dirección de red de la estación (IP:Puerto)
    pub direccion: String,
}

/// Actor que gestiona el servicio GPS.
///
/// Mantiene un registro de estaciones y responde a consultas de vehículos
/// con la dirección de la estación más cercana dentro del radio de búsqueda.
pub struct Gps {
    /// Lista de estaciones registradas
    estaciones: Vec<InfoEstacion>,
}

impl Gps {
    /// Crea un nuevo GPS con las estaciones especificadas.
    pub fn new(estaciones: Vec<InfoEstacion>) -> Self {
        for estacion in &estaciones {
            info!(
                "GPS: Registrando estación en ({}, {}) - Dirección: {}",
                estacion.coordenadas.latitud, estacion.coordenadas.longitud, estacion.direccion
            );
        }
        Self { estaciones }
    }

    /// Encuentra la estación más cercana a las coordenadas dadas dentro del radio.
    ///
    /// Retorna la dirección y distancia de la estación más cercana, o None si no hay ninguna en el radio.
    fn encontrar_estacion_cercana(&self, coord: Coordenadas) -> Option<(String, f64)> {
        let mut mejor: Option<(String, f64)> = None;

        for est in &self.estaciones {
            let distancia = coord.distancia_a(&est.coordenadas);

            if distancia <= RADIO_KM {
                mejor = match mejor {
                    None => Some((est.direccion.clone(), distancia)),
                    Some((_, dist_actual)) if distancia < dist_actual => {
                        Some((est.direccion.clone(), distancia))
                    }
                    other => other,
                }
            }
        }

        mejor
    }
}

impl Actor for Gps {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        info!("GPS: Iniciado");
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        info!("GPS: Detenido");
    }
}

impl Handler<MensajeRecibido<CommunicationHandler<Gps>>> for Gps {
    type Result = ();

    fn handle(
        &mut self,
        msg: MensajeRecibido<CommunicationHandler<Gps>>,
        _: &mut Self::Context,
    ) -> Self::Result {
        match msg.tipo_mensaje.as_str() {
            "SolicitarDireccionEstacion" => {
                match serde_json::from_str::<SolicitarDireccionEstacion>(&msg.payload) {
                    Ok(solicitud) => {
                        info!("GPS: Vehículo solicita estación cercana");

                        let resultado =
                            self.encontrar_estacion_cercana(solicitud.coordenadas_actuales);

                        let respuesta = match resultado {
                            Some((direccion, distancia)) => {
                                info!(
                                    "GPS: Estación encontrada ({:.2} km) - {}",
                                    distancia, direccion
                                );

                                RespuestaDireccionEstacion {
                                    direccion: Some(direccion),
                                }
                            }
                            None => {
                                warn!("GPS: No hay estaciones en rango");
                                RespuestaDireccionEstacion { direccion: None }
                            }
                        };

                        if let Ok(payload) = serde_json::to_string(&respuesta) {
                            msg.direccion.do_send(EnviarMensaje {
                                tipo_mensaje: "RespuestaDireccionEstacion".to_string(),
                                payload,
                            });
                        }
                    }
                    Err(e) => {
                        error!("🛰️ Error deserializando SolicitarDireccionEstacion: {}", e);
                    }
                }
            }

            _ => warn!("GPS: Tipo de mensaje desconocido {}", msg.tipo_mensaje),
        }
    }
}

impl Handler<ErrorConexion<CommunicationHandler<Gps>>> for Gps {
    type Result = ();

    fn handle(
        &mut self,
        msg: ErrorConexion<CommunicationHandler<Gps>>,
        _: &mut Self::Context,
    ) -> Self::Result {
        error!("GPS: Error de conexión: {}", msg.descripcion);
    }
}

impl Handler<ActorConnected<network::CommunicationHandler<Self>>> for Gps {
    type Result = ();

    fn handle(
        &mut self,
        msg: ActorConnected<network::CommunicationHandler<Self>>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        if msg.actor_type != ActorType::Vehiculo {
            warn!(
                "GPS: Conexión inesperada de {:?} (id: {})",
                msg.actor_type, msg.id
            );
            return;
        }
        info!(
            "GPS: Vehículo {} conectado desde {}",
            msg.id, msg.direccion_remota
        );
    }
}
