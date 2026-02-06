//! Biblioteca de tipos y utilidades compartidas para el sistema distribuido.
//!
//! Proporciona tipos de datos fundamentales, mensajes de comunicación y estructuras
//! utilizadas por todos los componentes del sistema.

pub mod messages;
pub mod types;

pub use messages::*;
pub use types::*;

/// Parsea un argumento de línea de comandos a un tipo genérico.
///
/// Convierte una cadena a tipo `T` o termina el programa con error si falla.
pub fn parse_arg<T: std::str::FromStr>(arg: &str, name: &str) -> T {
    arg.parse::<T>().unwrap_or_else(|_| {
        eprintln!("Error: {} inválido: {}", name, arg);
        std::process::exit(1);
    })
}
