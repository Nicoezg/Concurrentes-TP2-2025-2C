//! Tipos y estructuras para el administrador de compañía.
//!
//! Define los mensajes de comando, el actor principal y estructuras de datos
//! para gestionar la comunicación con el líder del cluster.

use actix::{Addr, Message};
use common::NodeInfo;
use network::CommunicationHandler;

/// Comando para definir el límite de gasto de la compañía.
#[derive(Message)]
#[rtype(result = "()")]
pub struct ComandoDefinirLimiteCompania {
    /// Nuevo límite de gasto en pesos
    pub limite: f64,
}

/// Comando para definir el límite de gasto de un vehículo específico.
#[derive(Message)]
#[rtype(result = "()")]
pub struct ComandoDefinirLimiteVehiculo {
    /// ID del vehículo
    pub vehiculo_id: u32,
    /// Nuevo límite de gasto en pesos
    pub limite: f64,
}

/// Comando para consultar el saldo disponible de la compañía.
#[derive(Message)]
#[rtype(result = "()")]
pub struct ComandoConsultarSaldoCompania;

/// Comando para consultar el saldo disponible de un vehículo.
#[derive(Message)]
#[rtype(result = "()")]
pub struct ComandoConsultarSaldoVehiculo {
    /// ID del vehículo
    pub vehiculo_id: u32,
}

/// Comando para solicitar el reporte mensual de transacciones.
#[derive(Message)]
#[rtype(result = "()")]
pub struct ComandoSolicitarReporteMensual;

/// Mensaje interno para iniciar descubrimiento del líder del cluster.
#[derive(Message)]
#[rtype(result = "()")]
pub struct DescubrirLider;

/// Mensaje interno para conectar al líder descubierto.
#[derive(Message)]
#[rtype(result = "()")]
pub struct ConectarLider {
    /// Dirección del líder
    pub leader_address: String,
}

/// Mensaje interno para establecer el handler de comunicación con el líder.
#[derive(Message)]
#[rtype(result = "()")]
pub struct EstablecerHandlerLider {
    /// Handler de comunicación
    pub handler: Addr<CommunicationHandler<crate::AdministradorDeCompania>>,
    /// Dirección del líder
    pub leader_address: String,
}

/// Comando pendiente de ser enviado al líder.
#[derive(Clone)]
pub enum ComandoPendiente {
    /// Definir límite de compañía
    DefinirLimiteCompania { limite: f64 },
    /// Definir límite de vehículo
    DefinirLimiteVehiculo { vehiculo_id: u32, limite: f64 },
    /// Consultar saldo de compañía
    ConsultarSaldoCompania,
    /// Consultar saldo de vehículo
    ConsultarSaldoVehiculo { vehiculo_id: u32 },
    /// Solicitar reporte mensual
    SolicitarReporteMensual,
}

/// Actor que gestiona las operaciones administrativas de una compañía.
///
/// Se conecta al líder del cluster y envía comandos para consultar y modificar
/// el estado de la cuenta de la compañía.
pub struct AdministradorDeCompania {
    /// ID de la compañía
    pub(crate) compania_id: u32,
    /// Información de todos los nodos del cluster
    pub(crate) cluster_nodes: Vec<NodeInfo>,
    /// Handler de comunicación con el líder actual
    pub(crate) handler_lider: Option<Addr<CommunicationHandler<Self>>>,
    /// Dirección del líder actual
    pub(crate) lider_actual: Option<String>,
    /// Buffer de comandos pendientes durante descubrimiento
    pub(crate) comandos_pendientes: Vec<ComandoPendiente>,
    /// Último comando enviado (para reintento en caso de fallo)
    pub(crate) ultimo_comando_enviado: Option<ComandoPendiente>,
}

impl AdministradorDeCompania {
    /// Crea un nuevo administrador para la compañía especificada.
    pub fn new(compania_id: u32, cluster_nodes: Vec<NodeInfo>) -> Self {
        Self {
            compania_id,
            cluster_nodes,
            handler_lider: None,
            lider_actual: None,
            comandos_pendientes: Vec::new(),
            ultimo_comando_enviado: None,
        }
    }
}
