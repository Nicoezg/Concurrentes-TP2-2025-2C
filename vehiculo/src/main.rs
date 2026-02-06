//! Proceso principal del vehículo que solicita cargas de combustible.
//!
//! Este proceso se conecta al GPS para obtener direcciones de estaciones cercanas,
//! y luego se conecta a las estaciones para solicitar cargas de combustible.

mod vehiculo;

use actix::Actor;
use common::{parse_arg, Coordenadas};
use log::{info, warn};
use network::CommunicationHandler;
use std::time::Duration;
use std::{env, io, thread};
use tokio::net::TcpStream;
use vehiculo::{SetHandlerGps, SolicitarDireccionEstacionInterno, Vehiculo};

/// Función principal del proceso vehículo.
#[actix_rt::main]
async fn main() {
    env_logger::init();
    info!("Proceso Vehículo: Iniciando proceso");

    let args: Vec<String> = env::args().collect();
    if args.len() != 5 {
        eprintln!(
            "Proceso Vehículo: Uso: {} <id> <compania_id> <latitud> <longitud>",
            args[0]
        );
        std::process::exit(1);
    }

    let id: u32 = parse_arg(&args[1], "id");
    let compania_id: u32 = parse_arg(&args[2], "compania_id");
    let latitud: i32 = parse_arg(&args[3], "latitud");
    let longitud: i32 = parse_arg(&args[4], "longitud");

    let coordenadas = Coordenadas::new(latitud, longitud);

    let vehiculo_addr = Vehiculo::new(id, compania_id, coordenadas).start();
    info!(
        "Proceso Vehículo {}: Iniciado en coordenadas ({}, {})",
        id, latitud, longitud
    );

    println!("Proceso Vehículo: Ingrese IP:PUERTO del GPS:");
    let mut gps_input = String::new();
    io::stdin()
        .read_line(&mut gps_input)
        .expect("Proceso Vehículo: No se pudo leer la entrada");

    let gps_addr = gps_input.trim().to_string();
    if gps_addr.is_empty() {
        eprintln!("Proceso Vehículo: Dirección del GPS vacía. Abortando.");
        std::process::exit(1);
    }

    info!(
        "Proceso Vehículo {}: Conectando al GPS ({})...",
        id, gps_addr
    );

    let handler_gps = match TcpStream::connect(&gps_addr).await {
        Ok(stream) => {
            info!("Proceso Vehículo {}: Conectado al GPS", id);
            Some(
                CommunicationHandler::new_initiator(
                    stream,
                    gps_addr.clone(),
                    vehiculo_addr.clone(),
                    common::ActorType::Vehiculo,
                    id,
                )
                .start(),
            )
        }
        Err(e) => {
            warn!("Proceso Vehículo {}: Error conectando al GPS: {}", id, e);
            None
        }
    };

    if let Some(gps) = handler_gps {
        vehiculo_addr.do_send(SetHandlerGps(gps));

        tokio::time::sleep(Duration::from_millis(500)).await;

        let addr_para_cli = vehiculo_addr.clone();

        println!("Ingrese la cantidad de combustible a cargar (o 'exit' para salir):");

        thread::spawn(move || {
            let stdin = io::stdin();
            loop {
                let mut buffer = String::new();
                if stdin.read_line(&mut buffer).is_ok() {
                    let input = buffer.trim();

                    if input.eq_ignore_ascii_case("exit") {
                        std::process::exit(0);
                    }

                    if let Ok(cantidad) = input.parse::<f64>() {
                        if cantidad > 0.0 {
                            addr_para_cli.do_send(SolicitarDireccionEstacionInterno { cantidad });
                        } else {
                            println!("Error: La cantidad debe ser mayor a 0.");
                        }
                    } else if !input.is_empty() {
                        println!("Error: Entrada inválida, ingrese un número.");
                    }
                }
            }
        });
    } else {
        warn!(
            "Proceso Vehículo {}: No se pudo conectar al GPS, el vehículo no puede solicitar cargas",
            id
        );
    }

    info!(
        "Proceso Vehículo {}: Ejecutando. Presiona Ctrl+C para detener",
        id
    );
    tokio::signal::ctrl_c().await.unwrap();
    info!("Proceso Vehículo {}: Detenido", id);
}
