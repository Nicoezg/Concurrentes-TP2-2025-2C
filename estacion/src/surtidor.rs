//! Actor que representa un surtidor de combustible.
//!
//! Solicita validación al cluster antes de realizar cargas y simula el proceso
//! de carga una vez aprobada la transacción.

use crate::administrador_red::AdministradorDeRed;
use crate::estacion::{EstacionDeServicio, SolicitudCargaPendiente};
use actix::{Actor, Addr, AsyncContext, Context, Handler};
use common::{
    CargaCompletada, IniciarCarga, RegistrarTransaccion, RegistroTransaccion, ResultadoValidacion,
};
use log::{info, warn};
use network::CommunicationHandler;
use std::time::Duration;

/// Precio del combustible por litro en pesos.
const PRECIO_POR_LITRO: f64 = 100.0;

/// Actor que representa un surtidor de combustible.
///
/// Gestiona cargas individuales, solicitando validación al cluster antes
/// de iniciar la carga física y notificando a la estación al completarla.
pub struct Surtidor {
    /// ID único del surtidor
    id: u32,
    /// Dirección de la estación padre
    addr_estacion: Option<Addr<crate::estacion::EstacionDeServicio>>,
    /// Dirección del administrador de red
    addr_admin_red: Option<Addr<AdministradorDeRed>>,
    /// Indica si está procesando una carga
    ocupado: bool,
    /// Solicitud de carga pendiente de validación
    carga_pendiente: Option<SolicitudCargaPendiente>,
}

impl Surtidor {
    /// Crea un nuevo surtidor con el ID especificado.
    pub fn new(id: u32) -> Self {
        Self {
            id,
            addr_estacion: None,
            addr_admin_red: None,
            ocupado: false,
            carga_pendiente: None,
        }
    }

    /// Configura la dirección de la estación padre.
    pub fn set_estacion(&mut self, addr: Addr<crate::estacion::EstacionDeServicio>) {
        self.addr_estacion = Some(addr);
    }

    /// Configura la dirección del administrador de red.
    pub fn set_admin_red(&mut self, addr: Addr<AdministradorDeRed>) {
        self.addr_admin_red = Some(addr);
    }

    /// Simula el proceso físico de carga de combustible.
    ///
    /// Programa la finalización de la carga basándose en la cantidad solicitada.
    fn simular_carga(&mut self, solicitud: SolicitudCargaPendiente, _ctx: &mut Context<Self>) {
        info!(
            "Surtidor {}: Iniciando carga REAL de {} litros para vehículo {} (confirmación recibida)",
            self.id, solicitud.cantidad, solicitud.vehiculo_id
        );

        let surtidor_id = self.id;
        let addr_estacion = self.addr_estacion.clone();
        let tiempo_carga = Duration::from_millis((solicitud.cantidad * 100.0) as u64);
        let vehiculo_id = solicitud.vehiculo_id;
        let addr_vehiculo = solicitud.addr_vehiculo;

        _ctx.run_later(tiempo_carga, move |act, _ctx| {
            info!(
                "Surtidor {}: Carga completada para vehículo {}",
                surtidor_id, vehiculo_id
            );

            if let Some(estacion) = addr_estacion {
                estacion.do_send(CargaCompletada {
                    exitosa: true,
                    addr_vehiculo: addr_vehiculo.clone(),
                    surtidor_id: act.id,
                });
            }

            act.ocupado = false;
        });
    }
}

impl Actor for Surtidor {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Self::Context) {
        info!("Surtidor {}: Iniciado", self.id);
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        info!("Surtidor {}: Detenido", self.id);
    }
}

impl Handler<IniciarCarga<CommunicationHandler<EstacionDeServicio>>> for Surtidor {
    type Result = ();

    fn handle(
        &mut self,
        msg: IniciarCarga<CommunicationHandler<EstacionDeServicio>>,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        if self.ocupado {
            warn!(
                "Surtidor {}: Ya está ocupado, no puede iniciar nueva carga",
                self.id
            );
            return;
        }

        info!(
            "Surtidor {}: Solicitando validación al cluster para vehículo {} ({} litros)",
            self.id, msg.vehiculo_id, msg.cantidad
        );

        self.ocupado = true;
        self.carga_pendiente = Some(SolicitudCargaPendiente {
            vehiculo_id: msg.vehiculo_id,
            compania_id: msg.compania_id,
            cantidad: msg.cantidad,
            contador_carga: msg.contador_carga,
            addr_vehiculo: msg.addr_vehiculo,
        });

        if let Some(admin) = &self.addr_admin_red {
            let monto = msg.cantidad * PRECIO_POR_LITRO;

            let transaccion = RegistroTransaccion {
                id: String::new(),
                vehiculo_id: msg.vehiculo_id,
                compania_id: msg.compania_id,
                monto,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                surtidor_id: self.id,
                contador_carga: msg.contador_carga,
            };

            admin.do_send(RegistrarTransaccion { transaccion });
        }
    }
}

impl Handler<ResultadoValidacion> for Surtidor {
    type Result = ();

    fn handle(&mut self, msg: ResultadoValidacion, _ctx: &mut Self::Context) -> Self::Result {
        if msg.aprobada {
            info!("Surtidor {}: Validación aprobada por el cluster", self.id);

            if let Some(solicitud) = self.carga_pendiente.take() {
                self.simular_carga(solicitud, _ctx);
            } else {
                warn!("Surtidor {}: No hay carga pendiente para procesar", self.id);
            }
        } else {
            warn!("Surtidor {}: Carga rechazada por el cluster", self.id);

            let solicitud = self.carga_pendiente.take();

            self.ocupado = false;

            if let Some(estacion) = &self.addr_estacion {
                if let Some(sol) = solicitud {
                    estacion.do_send(CargaCompletada {
                        exitosa: false,
                        addr_vehiculo: sol.addr_vehiculo,
                        surtidor_id: self.id,
                    });
                }
            }
        }
    }
}

/// Mensaje para configurar la dirección de la estación padre.
pub struct SetEstacion(pub Addr<crate::estacion::EstacionDeServicio>);

impl actix::Message for SetEstacion {
    type Result = ();
}

impl Handler<SetEstacion> for Surtidor {
    type Result = ();

    fn handle(&mut self, msg: SetEstacion, _: &mut Self::Context) -> Self::Result {
        info!("Surtidor {}: Dirección de estación configurada", self.id);
        self.set_estacion(msg.0);
    }
}
