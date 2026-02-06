use crate::admin_types::{AdministradorDeCompania, ConectarLider};
use actix::{AsyncContext, Handler};
use common::{LeaderInfo, MensajeRecibido, ReporteMensual, ResponderSaldo, TipoConsulta};
use log::{error, info, warn};
use network::CommunicationHandler;

impl Handler<MensajeRecibido<CommunicationHandler<AdministradorDeCompania>>>
    for AdministradorDeCompania
{
    type Result = ();

    fn handle(
        &mut self,
        msg: MensajeRecibido<CommunicationHandler<AdministradorDeCompania>>,
        ctx: &mut Self::Context,
    ) -> Self::Result {
        match msg.tipo_mensaje.as_str() {
            "RESPONDER_SALDO" => {
                // Limpiar último comando enviado ya que recibimos respuesta
                self.ultimo_comando_enviado = None;

                match serde_json::from_str::<ResponderSaldo>(&msg.payload) {
                    Ok(respuesta) => match respuesta.tipo {
                        TipoConsulta::Compania(compania_id) => {
                            info!(
                                "Admin: Saldo de compañía {}: ${:.2}",
                                compania_id, respuesta.saldo
                            );
                        }
                        TipoConsulta::Vehiculo(vehiculo_id) => {
                            info!(
                                "Admin: Saldo de vehículo {}: ${:.2}",
                                vehiculo_id, respuesta.saldo
                            );
                        }
                    },
                    Err(e) => {
                        error!("Error deserializando ResponderSaldo: {}", e);
                    }
                }
            }
            "REPORTE_MENSUAL" => {
                // Limpiar último comando enviado ya que recibimos respuesta
                self.ultimo_comando_enviado = None;

                match serde_json::from_str::<ReporteMensual>(&msg.payload) {
                    Ok(reporte) => {
                        info!(
                            "Admin: Reporte Mensual - Consumo total: ${:.2}",
                            reporte.consumo_total
                        );

                        for (vehiculo_id, consumo) in reporte.consumo_por_vehiculo {
                            info!("  Vehículo {}: ${:.2}", vehiculo_id, consumo);
                        }
                    }
                    Err(e) => {
                        error!("Error deserializando ReporteMensual: {}", e);
                    }
                }
            }
            "LEADER_INFO" => {
                match serde_json::from_str::<LeaderInfo>(&msg.payload) {
                    Ok(leader_info) => {
                        info!(
                            "Admin {}: Recibida información del líder en {}",
                            self.compania_id, leader_info.leader_address
                        );

                        // Enviar mensaje para reconectar al líder
                        ctx.address().do_send(ConectarLider {
                            leader_address: leader_info.leader_address,
                        });
                    }
                    Err(e) => {
                        error!("Error deserializando LeaderInfo: {}", e);
                    }
                }
            }
            _ => {
                warn!("Admin: Tipo de mensaje desconocido: {}", msg.tipo_mensaje);
            }
        }
    }
}
