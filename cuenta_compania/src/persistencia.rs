//! Sistema de persistencia para garantizar idempotencia de transacciones.
//!
//! Mantiene un registro de transacciones procesadas para evitar duplicados
//! usando un caché en memoria respaldado por persistencia en disco.

use log::{error, info};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::Path;

/// Almacén de IDs de transacciones procesadas para garantizar idempotencia.
///
/// Mantiene un conjunto de IDs en memoria respaldado por un archivo en disco
/// para sobrevivir reinicios del nodo.
pub struct AlmacenIdempotencia {
    /// Ruta del archivo de persistencia
    ruta: String,
    /// Conjunto de IDs ya procesados
    ids_procesados: HashSet<String>,
}

impl AlmacenIdempotencia {
    /// Crea un nuevo almacén cargando IDs previos desde disco.
    pub fn new(id_compania: u32) -> io::Result<Self> {
        let ruta = format!("log_idempotencia_compania_{}.jsonl", id_compania);
        let mut almacen = Self {
            ruta,
            ids_procesados: HashSet::new(),
        };
        almacen.cargar_ids()?;
        Ok(almacen)
    }

    /// Carga los IDs procesados desde el archivo en disco.
    fn cargar_ids(&mut self) -> io::Result<()> {
        let path = Path::new(&self.ruta);
        if !path.exists() {
            return Ok(());
        }

        let file = File::open(path)?;
        let reader = io::BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            match serde_json::from_str::<String>(&line) {
                Ok(id) => {
                    self.ids_procesados.insert(id);
                }
                Err(e) => {
                    error!("[AlmacenIdempotencia] Línea corrupta ignorada: {}", e);
                }
            }
        }
        info!(
            "[AlmacenIdempotencia] Cargados {} IDs de transacciones procesadas.",
            self.ids_procesados.len()
        );
        Ok(())
    }

    /// Verifica si un ID de transacción ya fue procesado.
    pub fn esta_procesado(&self, id: &str) -> bool {
        self.ids_procesados.contains(id)
    }

    /// Marca un ID como procesado y lo guarda en disco.
    pub fn marcar_como_procesado(&mut self, id: String, estado: bool) -> io::Result<()> {
        if self.ids_procesados.insert(id.clone()) {
            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&self.ruta)?;
            let data = serde_json::json!({"id": id, "estado": estado});
            let mut json = serde_json::to_string(&data)?;
            json.push('\n');
            file.write_all(json.as_bytes())?;
        }
        Ok(())
    }

    /// Obtener el estado de una transacción procesada
    pub fn obtener_datos_transaccion(&self, id: &str) -> io::Result<bool> {
        let path = Path::new(&self.ruta);
        if !path.exists() {
            return Ok(false);
        }

        let file = File::open(path)?;
        let reader = io::BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            match serde_json::from_str::<serde_json::Value>(&line) {
                Ok(json) => {
                    if let Some(id_transaccion) = json.get("id").and_then(|v| v.as_str()) {
                        if id_transaccion == id {
                            if let Some(estado) = json.get("estado").and_then(|v| v.as_bool()) {
                                return Ok(estado);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "[AlmacenIdempotencia] Línea corrupta ignorada al buscar transacción: {}",
                        e
                    );
                }
            }
        }
        Ok(false)
    }
}
