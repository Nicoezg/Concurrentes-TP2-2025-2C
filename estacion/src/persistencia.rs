//! Sistema de persistencia para transacciones (Store and Forward).
//!
//! Implementa el patrón Store and Forward, guardando transacciones en disco
//! antes de enviarlas al cluster y manteniendo un caché en memoria para acceso rápido.

use common::RegistroTransaccion;
use log::error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Estado de una transacción en el sistema Store and Forward.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum EstadoTransaccion {
    /// Transacción guardada pero no confirmada por el cluster
    Pendiente,
    /// Transacción confirmada por el cluster
    Confirmada,
}

/// Registro de una transacción con su estado y metadata.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogTransaccion {
    /// ID único de la transacción
    pub id: String,
    /// Estado actual de la transacción
    pub estado: EstadoTransaccion,
    /// Datos de la transacción
    pub transaccion: RegistroTransaccion,
    /// Timestamp del último envío
    #[serde(default)]
    pub timestamp_envio: u64,
    /// Resultado de validación del cluster
    #[serde(default)]
    pub aprobada: Option<bool>,
}

/// Logger de transacciones con persistencia en disco y caché en memoria.
///
/// Implementa el patrón Store and Forward, garantizando que las transacciones
/// no se pierdan aunque la estación pierda conexión con el cluster.
pub struct TransactionLogger {
    /// Ruta del archivo de log
    path: String,
    /// Caché en memoria de todas las transacciones
    cache: HashMap<String, LogTransaccion>,
}

impl TransactionLogger {
    /// Crea un nuevo logger cargando el estado desde disco.
    ///
    /// Lee todas las transacciones previamente guardadas y las carga en memoria.
    pub fn new(id_estacion: u32) -> io::Result<Self> {
        let path_str = format!("log_transacciones_estacion_{}.jsonl", id_estacion);
        let path = Path::new(&path_str);

        let mut cache = HashMap::new();

        if path.exists() {
            let file = File::open(path)?;
            let reader = io::BufReader::new(file);

            for line in reader.lines() {
                let line = line?;
                match serde_json::from_str::<LogTransaccion>(&line) {
                    Ok(log) => {
                        cache.insert(log.id.clone(), log);
                    }
                    Err(e) => {
                        error!(
                            "TransactionLogger: error deserializando línea en carga inicial: {} -- error: {}",
                            line, e
                        );
                    }
                }
            }
        }

        Ok(Self {
            path: path_str,
            cache,
        })
    }

    /// Guarda una transacción en disco y actualiza el caché en memoria.
    ///
    /// Usa append-only para garantizar durabilidad.
    pub fn log(&mut self, log: &LogTransaccion) -> io::Result<()> {
        let mut log_to_save = log.clone();
        log_to_save.timestamp_envio = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)?;

        let mut json = serde_json::to_string(&log_to_save)?;
        json.push('\n');
        file.write_all(json.as_bytes())?;

        self.cache.insert(log_to_save.id.clone(), log_to_save);

        Ok(())
    }

    /// Obtiene todas las transacciones con un estado específico desde el caché.
    pub fn get_transacciones_por_estado(
        &self,
        estado_buscado: EstadoTransaccion,
    ) -> Vec<LogTransaccion> {
        self.cache
            .values()
            .filter(|log| log.estado == estado_buscado)
            .cloned()
            .collect()
    }

    /// Busca una transacción específica por ID en el caché.
    pub fn get_transaccion(&self, id: &str) -> Option<&LogTransaccion> {
        self.cache.get(id)
    }
}
