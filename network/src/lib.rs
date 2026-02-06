//! Biblioteca de comunicación TCP para el sistema distribuido.
//!
//! Proporciona componentes para establecer y manejar conexiones TCP asíncronas
//! entre los diferentes actores del sistema usando Actix.

pub mod acceptor;
pub mod communication_handler;

pub use acceptor::Acceptor;
pub use communication_handler::CommunicationHandler;
