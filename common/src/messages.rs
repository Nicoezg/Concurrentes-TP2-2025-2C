//! Definición de mensajes para comunicación entre actores.
//!
//! Este módulo contiene todos los tipos de mensajes intercambiados entre
//! los diferentes componentes del sistema distribuido utilizando el framework Actix.

use crate::types::{Coordenadas, EstadoCompania, RegistroTransaccion};
use actix::Message;
use serde::{Deserialize, Serialize};

// ============================================================================
// Mensajes entre Vehículo y GPS
// ============================================================================

/// Solicitud de dirección de estación más cercana.
///
/// Enviado por un vehículo al GPS para obtener la dirección de la estación
/// de servicio más cercana a sus coordenadas actuales.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct SolicitarDireccionEstacion {
    /// Coordenadas actuales del vehículo
    pub coordenadas_actuales: Coordenadas,
}

/// Respuesta con la dirección de la estación más cercana.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct RespuestaDireccionEstacion {
    /// Dirección de la estación en formato IP:Puerto, o None si no hay estaciones disponibles
    pub direccion: Option<String>,
}

// ============================================================================
// Mensajes entre Vehículo y Estación de Servicio
// ============================================================================

/// Solicitud de carga de combustible de un vehículo.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct SolicitarCarga {
    /// ID del vehículo
    pub vehiculo_id: u32,
    /// ID de la compañía
    pub compania_id: u32,
    /// Cantidad de litros a cargar
    pub cantidad: f64,
    /// Contador de carga para idempotencia
    pub contador_carga: u32,
}

/// Respuesta a una solicitud de carga.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct RespuestaCarga {
    /// Indica si la carga fue aceptada o rechazada
    pub aceptada: bool,
}

// ============================================================================
// Mensajes Internos de Estación de Servicio
// ============================================================================

/// Mensaje interno para iniciar el proceso de carga en un surtidor.
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct IniciarCarga<T: actix::Actor> {
    pub vehiculo_id: u32,
    pub compania_id: u32,
    pub cantidad: f64,
    pub contador_carga: u32,
    /// Dirección del actor vehículo para enviar la respuesta
    pub addr_vehiculo: actix::Addr<T>,
}

/// Solicitud para registrar una transacción en el clúster.
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct RegistrarTransaccion {
    pub transaccion: RegistroTransaccion,
}

/// Resultado de la validación de una transacción por el clúster.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct ResultadoValidacion {
    /// Indica si la transacción fue aprobada
    pub aprobada: bool,
}

/// Resultado completo de una transacción procesada.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct ResultadoTransaccionEstacion {
    pub transaccion: RegistroTransaccion,
    pub aprobada: bool,
}

/// Notificación de finalización de carga desde un surtidor.
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct CargaCompletada<T: actix::Actor> {
    /// Indica si la carga fue exitosa
    pub exitosa: bool,
    pub addr_vehiculo: actix::Addr<T>,
    pub surtidor_id: u32,
}

// ============================================================================
// Mensajes entre AdministradorDeRed y CuentaCompania
// ============================================================================

/// Solicitud para registrar una transacción en el clúster de forma remota.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "ResultadoTransaccion")]
pub struct RegistrarTransaccionRemota {
    pub transaccion: RegistroTransaccion,
}

/// Resultado del procesamiento de una transacción.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct ResultadoTransaccion {
    pub vehiculo_id: u32,
    /// Indica si la transacción fue aceptada o rechazada
    pub aceptada: bool,
}

/// Notificación de nueva transacción desde una estación.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct NuevaTransaccionEstacion {
    pub transaccion: RegistroTransaccion,
}

// ============================================================================
// Mensajes entre AdministradorDeCompania y CuentaCompania
// ============================================================================

/// Define el límite de gasto de una compañía.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct DefinirLimiteCompania {
    pub compania_id: u32,
    /// Límite en pesos
    pub limite: f64,
}

/// Define el límite de gasto de un vehículo específico.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct DefinirLimiteVehiculo {
    pub compania_id: u32,
    pub vehiculo_id: u32,
    /// Límite en pesos
    pub limite: f64,
}

/// Consulta el saldo disponible de un vehículo.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "ResponderSaldo")]
pub struct ConsultarSaldoVehiculo {
    pub vehiculo_id: u32,
}

/// Consulta el saldo disponible de una compañía.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "ResponderSaldo")]
pub struct ConsultarSaldoCompania {
    pub compania_id: u32,
}

/// Solicita el reporte mensual de consumo de una compañía.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "ReporteMensual")]
pub struct SolicitarReporteMensual {
    pub id_compania: u32,
}

/// Tipo de consulta de saldo (compañía o vehículo).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TipoConsulta {
    Compania(u32),
    Vehiculo(u32),
}

/// Respuesta con el saldo consultado.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct ResponderSaldo {
    /// Saldo disponible en pesos
    pub saldo: f64,
    pub tipo: TipoConsulta,
}

/// Reporte mensual de consumo.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct ReporteMensual {
    /// Consumo total de la compañía
    pub consumo_total: f64,
    /// Lista de tuplas (vehiculo_id, consumo)
    pub consumo_por_vehiculo: Vec<(u32, f64)>,
}

// ============================================================================
// Mensajes de Sincronización de Estado entre Líder y Réplica
// ============================================================================

/// Actualización del estado de consumo de líder a réplica.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct ActualizacionEstado {
    pub compania_id: u32,
    pub vehiculo_id: u32,
    pub consumo_compania: f64,
    pub consumo_vehiculo: f64,
}

// ============================================================================
// Mensajes de Descubrimiento de Líder
// ============================================================================

/// Información de un nodo en el clúster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: u32,
    pub address: String,
}

/// Consulta para descubrir quién es el líder actual.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct WhoIsLeader {
    pub requester_id: u32,
}

/// Respuesta con la información del líder.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct LeaderInfo {
    pub leader_address: String,
}

// ============================================================================
// Mensajes del Algoritmo Bully
// ============================================================================

/// Mensaje de elección en el algoritmo Bully.
///
/// Enviado por un nodo para iniciar o participar en una elección de líder.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "OkEleccion")]
pub struct Eleccion {
    pub id_cuenta: u32,
}

/// Respuesta afirmativa a un mensaje de elección.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct OkEleccion;

/// Mensaje de coordinador en el algoritmo Bully.
///
/// Anuncia que un nodo se ha convertido en el nuevo líder.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "ActualizarLider")]
pub struct Coordinador {
    pub id_cuenta_lider: u32,
}

/// Actualización del rol de un nodo tras elegirse un nuevo líder.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct ActualizarLider {
    /// Estado actual del cluster
    pub estado: EstadoCompania,
    /// Indica si este nodo es réplica
    pub es_backup: bool,
    /// Estado del líder anterior si existía
    pub estado_lider_anterior: Option<EstadoCompania>,
}

/// Mensaje ping del sistema de heartbeats.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct HeartbeatPing {
    pub id_nodo: u32,
    /// Timestamp UNIX en milisegundos
    pub timestamp: u64,
}

/// Respuesta pong del sistema de heartbeats.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct HeartbeatPong {
    pub id_nodo: u32,
    /// Timestamp UNIX en milisegundos
    pub timestamp: u64,
}

/// Inicia el sistema de heartbeats.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct IniciarHeartbeats;

/// Detiene el sistema de heartbeats.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct DetenerHeartbeats;

// ============================================================================
// Mensajes de Comunicación de Red
// ============================================================================

/// Solicitud para enviar un mensaje a través de una conexión TCP.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct EnviarMensaje {
    /// Tipo de mensaje (usado para dispatch)
    pub tipo_mensaje: String,
    /// Payload serializado en JSON
    pub payload: String,
}

/// Notificación de mensaje recibido desde una conexión TCP.
#[derive(Debug, Clone, Message)]
#[rtype(result = "()")]
pub struct MensajeRecibido<T: actix::Actor> {
    /// Tipo de mensaje recibido
    pub tipo_mensaje: String,
    /// Payload serializado en JSON
    pub payload: String,
    /// Dirección IP:Puerto del remitente
    pub direccion_remota: String,
    /// Handler para comunicarse con el remitente
    pub direccion: actix::Addr<T>,
}

/// Notificación de error en una conexión TCP.
#[derive(Clone, Message)]
#[rtype(result = "()")]
pub struct ErrorConexion<T: actix::Actor> {
    /// Descripción del error
    pub descripcion: String,
    /// Handler de la conexión que falló
    pub handler: actix::Addr<T>,
}

/// Solicitud para cerrar una conexión TCP.
#[derive(Debug, Clone, Serialize, Deserialize, Message)]
#[rtype(result = "()")]
pub struct CerrarConexion {
    /// Motivo del cierre
    pub motivo: String,
}

/// Notificación de que un nuevo actor se ha conectado.
#[derive(Clone, Message)]
#[rtype(result = "()")]
pub struct ActorConnected<T>
where
    T: actix::Actor,
{
    /// Tipo de actor conectado
    pub actor_type: crate::ActorType,
    /// Dirección IP:Puerto del actor
    pub direccion_remota: String,
    /// ID del actor
    pub id: u32,
    /// Handler para comunicarse con el actor
    pub handler: actix::Addr<T>,
}
