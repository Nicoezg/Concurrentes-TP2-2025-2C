//! Actor principal del nodo de cuenta de compañía.
//!
//! Implementa la lógica de consenso (Bully), gestión de estado de cuentas,
//! procesamiento de transacciones y replicación de estado.

use crate::heartbeat_manager::{AgregarNodo, HeartbeatManager};
use crate::persistencia::AlmacenIdempotencia;
use actix::{Addr, Message};
use common::{EstadoCompania, RegistroTransaccion, RolNodo};
use log::info;
use network::CommunicationHandler;
use std::collections::HashMap;

mod actor_impls;
mod business_logic;
mod message_handlers;

/// Mensaje para iniciar una elección de líder.
#[derive(Message)]
#[rtype(result = "()")]
pub struct IniciarEleccion;

/// Mensaje interno para notificar fallo de un nodo.
#[derive(Message)]
#[rtype(result = "()")]
pub struct NotificarFalloNodo {}

/// Transacción pendiente de ser procesada con su handler de respuesta.
pub(crate) struct TransaccionPendiente {
    /// Datos de la transacción
    pub transaccion: RegistroTransaccion,
    /// Handler para enviar la respuesta
    pub manejador_respuesta: Option<Addr<CommunicationHandler<CuentaCompania>>>,
}

/// Actor que representa un nodo del cluster de cuentas de compañía.
///
/// Mantiene el estado de las cuentas, participa en el algoritmo de consenso Bully,
/// procesa transacciones (si es líder) y replica estado (si es réplica).
pub struct CuentaCompania {
    /// ID único del nodo
    pub(crate) id: u32,
    /// Rol actual en el cluster
    pub(crate) rol: RolNodo,
    /// Buffer de transacciones para envío por lotes al líder
    pub(crate) buffer_transacciones: Option<Vec<TransaccionPendiente>>,
    /// Tamaño máximo del buffer antes de enviar
    pub(crate) n_transacciones: usize,
    /// Estado global de las cuentas (si es líder)
    pub(crate) estado_global: HashMap<u32, EstadoCompania>,
    /// Copia del estado global (si es réplica)
    pub(crate) estado_global_replica: HashMap<u32, EstadoCompania>,
    /// Nodos vecinos conectados
    pub(crate) nodos_vecinos: HashMap<u32, Addr<CommunicationHandler<Self>>>,
    /// ID del nodo líder actual
    pub(crate) id_lider: Option<u32>,
    /// ID del nodo réplica (si este nodo es líder)
    pub(crate) id_replica: Option<u32>,
    /// Indica si está en proceso de elección
    pub(crate) en_eleccion: bool,
    /// Manager de heartbeats para detección de fallos
    pub(crate) heartbeat_manager: Option<Addr<HeartbeatManager>>,
    /// Dirección de red del líder
    pub(crate) address_lider: String,
    /// Almacén para garantizar idempotencia de transacciones
    pub(crate) almacen_idempotencia: AlmacenIdempotencia,
}

impl CuentaCompania {
    /// Crea un nuevo nodo con el ID y configuración especificados.
    pub fn new(id: u32, address_lider: String, buffer_size: usize) -> Self {
        let almacen_idempotencia = AlmacenIdempotencia::new(id).expect(
            "Error fatal: No se pudo inicializar o leer el almacén de idempotencia del nodo cuenta_compania",
        );

        Self {
            id,
            rol: RolNodo::Ninguno,
            buffer_transacciones: Some(Vec::new()),
            n_transacciones: buffer_size,
            estado_global: HashMap::new(),
            estado_global_replica: HashMap::new(),
            nodos_vecinos: HashMap::new(),
            id_lider: None,
            id_replica: None,
            en_eleccion: false,
            // HeartbeatManager será inicializado en started()
            heartbeat_manager: None,

            address_lider,
            almacen_idempotencia,
        }
    }

    /// Agrega un nodo vecino al cluster y lo registra en el heartbeat manager.
    pub fn agregar_vecino(&mut self, nodo_id: u32, handler: Addr<CommunicationHandler<Self>>) {
        info!("Nodo {}: Registrando vecino {}", self.id, nodo_id);
        self.nodos_vecinos.insert(nodo_id, handler.clone());

        // Delegar al HeartbeatManager
        if let Some(hb_manager) = &self.heartbeat_manager {
            hb_manager.do_send(AgregarNodo {
                id_nodo: nodo_id,
                handler,
            });
        }
    }
}
