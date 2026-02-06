mod gps;

use actix::Actor;
use common::{parse_arg, Coordenadas};
use gps::Gps;
use log::info;
use network::Acceptor;
use std::{env, io};

/* Función principal */
#[actix_rt::main]
async fn main() {
    info!("Proceso GPS: Iniciando");

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Uso: {} <IP:PUERTO>", args[0]);
        std::process::exit(1);
    }

    let bind_addr = args[1].clone();

    println!("Ingrese estaciones (latitud longitud IP:puerto). Línea vacía para terminar:");

    let mut estaciones = Vec::new();
    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        let input = input.trim();
        if input.is_empty() {
            break;
        }

        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.len() != 3 {
            println!("Error: se esperaban 3 valores (latitud longitud IP:puerto).");
            continue;
        }

        let latitud: i32 = parse_arg(parts[0], "Latitud");
        let longitud: i32 = parse_arg(parts[1], "Longitud");
        let direccion = parts[2].to_string();

        estaciones.push(gps::InfoEstacion {
            coordenadas: Coordenadas::new(latitud, longitud),
            direccion,
        });
    }

    let gps_addr = Gps::new(estaciones).start();

    let _acceptor = Acceptor::new(bind_addr.clone(), gps_addr.clone()).start();
    info!("Escuchando en {}", bind_addr);

    tokio::signal::ctrl_c().await.unwrap();
    info!("Proceso GPS detenido");
}
