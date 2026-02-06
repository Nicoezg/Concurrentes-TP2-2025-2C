//! Proceso principal del nodo de cuenta de compañía en el cluster.
//!
//! Implementa un nodo del cluster que mantiene el estado de las cuentas de compañías,
//! procesa transacciones y participa en el algoritmo de consenso basado en Bully.

mod cuenta_compania;
mod heartbeat_manager;
mod persistencia;

use actix::Actor;
use common::parse_arg;
use cuenta_compania::{CuentaCompania, IniciarEleccion};
use log::{info, warn};
use network::{Acceptor, CommunicationHandler};
use std::env;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

/// Flag que indica si el nodo está reingresando al cluster tras una caída.
const RECONNECTION_FLAG: &str = "1";

#[actix_rt::main]
async fn main() {
    env_logger::init();
    info!("Proceso CuentaCompania: Iniciando proceso");

    let args: Vec<String> = env::args().collect();

    if args.len() < 5 || args.len() > 6 {
        // programa, nodo_id, bind_addr, vecinos, buffer_size, [reconnection_flag]
        eprintln!(
            "Uso: {} <nodo_id> <bind_addr> <vecinos> <buffer_size> [reconnection_flag]",
            args[0]
        );
        std::process::exit(1);
    }

    let nodo_id: u32 = parse_arg(&args[1], "nodo_id");
    let bind_addr = &args[2];
    let vecinos_str = &args[3];
    let buffer_size: usize = parse_arg(&args[4], "buffer_size");
    let reconnection_flag = if args.len() == 6 { &args[5] } else { "0" };

    if nodo_id == 0 {
        std::process::exit(1);
    }

    let mut vecinos: Vec<(u32, String)> = Vec::new();

    if !vecinos_str.is_empty() {
        for vecino_info in vecinos_str.split(',') {
            if let Some(pos) = vecino_info.find(':') {
                let (id_str, addr) = vecino_info.split_at(pos);
                let peer_id: u32 = parse_arg(id_str, "peer_id");
                let peer_addr = &addr[1..];
                vecinos.push((peer_id, peer_addr.to_string()));
            } else {
                warn!(
                    "Proceso CuentaCompania: Formato de vecino inválido: {}",
                    vecino_info
                );
            }
        }
    }

    let cuenta = CuentaCompania::new(nodo_id, bind_addr.to_string(), buffer_size);

    let cuenta_addr = cuenta.start();

    info!("Proceso CuentaCompania: Nodo {} iniciado", nodo_id);

    info!("Proceso CuentaCompania: Escuchando en {}", bind_addr);

    let _acceptor = Acceptor::new(bind_addr.to_string(), cuenta_addr.clone()).start();

    sleep(Duration::from_millis(1000)).await;

    for (peer_id, peer_addr) in vecinos {
        if peer_id <= nodo_id && (reconnection_flag != RECONNECTION_FLAG && peer_id != nodo_id) {
            continue;
        }

        let mut intentos = 0;
        let max_intentos = 10;

        loop {
            match TcpStream::connect(&peer_addr).await {
                Ok(stream) => {
                    info!(
                        "Proceso CuentaCompania: Conectado a nodo {} en {}",
                        peer_id, peer_addr
                    );

                    let handler_addr = CommunicationHandler::new_initiator(
                        stream,
                        peer_addr.clone(),
                        cuenta_addr.clone(),
                        common::ActorType::NodoCompania,
                        nodo_id,
                    )
                    .start();

                    cuenta_addr.do_send(common::ActorConnected {
                        actor_type: common::ActorType::NodoCompania,
                        direccion_remota: peer_addr.clone(),
                        id: peer_id,
                        handler: handler_addr,
                    });

                    break;
                }
                Err(e) => {
                    intentos += 1;
                    if intentos >= max_intentos {
                        warn!("Proceso CuentaCompania: No se pudo conectar a nodo {} ({}) después de {} intentos: {}", peer_id, peer_addr, max_intentos, e);
                        break;
                    }
                    sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    info!(
        "Proceso CuentaCompania: Nodo {} listo y conectado al cluster",
        nodo_id
    );

    sleep(Duration::from_millis(500)).await;

    if nodo_id == 1 && reconnection_flag != RECONNECTION_FLAG {
        info!("Proceso CuentaCompania: Iniciando elección de líder con el nodo 1");
        cuenta_addr.do_send(IniciarEleccion);
    }

    tokio::signal::ctrl_c().await.unwrap();
    info!("Proceso CuentaCompania: Detenido");
}
