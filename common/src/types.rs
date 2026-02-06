//! Tipos de datos compartidos para el sistema distribuido.
//!
//! Este módulo define las estructuras de datos fundamentales utilizadas
//! en todo el sistema de gestión de estaciones de servicio distribuidas.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Coordenadas geográficas en grados.
///
/// Representa una ubicación geográfica usando latitud y longitud enteras.
/// Se utiliza para calcular distancias entre vehículos y estaciones.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Coordenadas {
    /// Latitud en grados (-90 a 90)
    pub latitud: i32,
    /// Longitud en grados (-180 a 180)
    pub longitud: i32,
}

impl Coordenadas {
    /// Crea una nueva instancia de coordenadas.
    pub fn new(latitud: i32, longitud: i32) -> Self {
        Self { latitud, longitud }
    }

    /// Calcula la distancia aproximada en kilómetros usando la fórmula de Haversine.
    pub fn distancia_a(&self, otra: &Coordenadas) -> f64 {
        const R: f64 = 6371.0; // Radio de la Tierra en km
        let lat1 = (self.latitud as f64).to_radians();
        let lon1 = (self.longitud as f64).to_radians();
        let lat2 = (otra.latitud as f64).to_radians();
        let lon2 = (otra.longitud as f64).to_radians();

        let dlat = lat2 - lat1;
        let dlon = lon2 - lon1;

        let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);

        let c = 2.0 * a.sqrt().asin();

        R * c
    }
}

/// Estado financiero de un vehículo individual.
///
/// Mantiene el registro del consumo acumulado y el límite de gasto asignado.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EstadoVehiculo {
    /// Monto total consumido por el vehículo en pesos
    pub consumo: f64,
    /// Límite máximo de gasto permitido en pesos
    pub limite: f64,
}

impl EstadoVehiculo {
    /// Crea un nuevo estado de vehículo con el límite especificado y consumo inicial en 0.
    pub fn new(limite: f64) -> Self {
        Self {
            consumo: 0.0,
            limite,
        }
    }

    /// Calcula el saldo disponible (límite - consumo).
    pub fn saldo_disponible(&self) -> f64 {
        self.limite - self.consumo
    }

    /// Verifica si el vehículo puede realizar una carga dado un monto.
    pub fn puede_cargar(&self, monto: f64) -> bool {
        self.consumo + monto <= self.limite
    }
}

/// Estado financiero de una compañía.
///
/// Gestiona el límite global de la compañía y los estados individuales
/// de todos sus vehículos. Valida transacciones contra ambos límites.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EstadoCompania {
    /// Mapeo de ID de vehículo a su estado financiero
    pub vehiculos: HashMap<u32, EstadoVehiculo>,
    /// Consumo total acumulado de todos los vehículos en pesos
    pub consumo: f64,
    /// Límite máximo de gasto de la compañía en pesos
    pub limite: f64,
}

impl EstadoCompania {
    /// Crea un nuevo estado de compañía con el límite especificado.
    pub fn new(limite: f64) -> Self {
        Self {
            vehiculos: HashMap::new(),
            consumo: 0.0,
            limite,
        }
    }

    /// Agrega un vehículo a la compañía con su límite individual.
    pub fn agregar_vehiculo(&mut self, vehiculo_id: u32, limite_vehiculo: f64) {
        self.vehiculos
            .insert(vehiculo_id, EstadoVehiculo::new(limite_vehiculo));
    }

    /// Verifica si un vehículo puede cargar un monto dado.
    ///
    /// Valida tanto el límite de la compañía como el límite individual del vehículo.
    pub fn puede_cargar(&self, vehiculo_id: u32, monto: f64) -> bool {
        // Verificar límite de la compañía
        if self.consumo + monto > self.limite {
            return false;
        }

        // Verificar límite del vehículo
        if let Some(vehiculo) = self.vehiculos.get(&vehiculo_id) {
            vehiculo.puede_cargar(monto)
        } else {
            false
        }
    }

    /// Registra un consumo para un vehículo específico.
    ///
    /// Actualiza tanto el consumo del vehículo como el consumo total de la compañía.
    /// Retorna `false` si la transacción excede algún límite.
    pub fn registrar_consumo(&mut self, vehiculo_id: u32, monto: f64) -> bool {
        if !self.puede_cargar(vehiculo_id, monto) {
            return false;
        }

        self.consumo += monto;
        if let Some(vehiculo) = self.vehiculos.get_mut(&vehiculo_id) {
            vehiculo.consumo += monto;
            true
        } else {
            false
        }
    }

    /// Calcula el saldo disponible de la compañía (límite - consumo).
    pub fn saldo_disponible(&self) -> f64 {
        self.limite - self.consumo
    }
}

/// Rol de un nodo en el clúster de consenso.
///
/// Define los roles posibles que puede tener un nodo en el algoritmo
/// de elección de líder basado en Bully.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RolNodo {
    /// Nodo coordinador que procesa transacciones
    Lider,
    /// Nodo réplica que mantiene copia del estado del líder
    Replica,
    /// Nodo sin rol asignado
    Ninguno,
}

/// Registro de una transacción de carga de combustible.
///
/// Utilizado en el patrón Store and Forward para persistir transacciones
/// antes de ser procesadas por el clúster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistroTransaccion {
    /// Identificador único de la transacción
    pub id: String,
    /// ID del vehículo que realiza la carga
    pub vehiculo_id: u32,
    /// ID de la compañía a la que pertenece el vehículo
    pub compania_id: u32,
    /// Monto de la transacción en pesos
    pub monto: f64,
    /// Timestamp UNIX en milisegundos
    pub timestamp: u64,
    /// ID del surtidor que realizó la carga
    pub surtidor_id: u32,
    /// Número de carga del vehículo (para idempotencia)
    pub contador_carga: u32,
}

/// Configuración del sistema de heartbeats.
///
/// Define los parámetros temporales para el mecanismo de detección
/// de fallos basado en heartbeats entre nodos del clúster.
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    /// Intervalo entre heartbeat pings
    pub ping_interval: Duration,
    /// Tiempo de espera para detectar fallo de nodo
    pub failure_timeout: Duration,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_millis(2000),   // 2 segundos
            failure_timeout: Duration::from_millis(6000), // 6 segundos (3 intervalos de ping)
        }
    }
}

/// Estado de heartbeat de un nodo.
///
/// Registra cuándo fue la última comunicación con un nodo y si está vivo.
#[derive(Debug, Clone)]
pub struct HeartbeatState {
    /// Última vez que se recibió un ping/pong
    pub last_ping_received: Instant,
    /// Indica si el nodo está vivo
    pub is_alive: bool,
}

impl Default for HeartbeatState {
    fn default() -> Self {
        Self::new()
    }
}

impl HeartbeatState {
    /// Crea un nuevo estado de heartbeat con timestamp actual.
    pub fn new() -> Self {
        Self {
            last_ping_received: Instant::now(),
            is_alive: true,
        }
    }

    /// Actualiza el estado al recibir un ping/pong del nodo.
    pub fn update_ping_received(&mut self) {
        self.last_ping_received = Instant::now();
        self.is_alive = true;
    }

    /// Verifica si el nodo ha excedido el tiempo de espera.
    pub fn is_timed_out(&self, timeout: Duration) -> bool {
        self.last_ping_received.elapsed() > timeout
    }

    /// Marca el nodo como fallido.
    pub fn mark_failed(&mut self) {
        self.is_alive = false;
    }
}

/// Tipo de actor en el sistema distribuido.
///
/// Identifica el rol de cada proceso en la arquitectura del sistema.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ActorType {
    /// Proceso vehículo que solicita cargas
    Vehiculo,
    /// Servicio GPS que proporciona direcciones de estaciones
    GPS,
    /// Nodo del clúster de cuenta de compañía
    NodoCompania,
    /// Administrador de compañía para consultas y configuración
    AdminCompania,
    /// Estación de servicio con surtidores
    Estacion,
}
