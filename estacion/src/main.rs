//! Proceso principal de la estación de servicio.
//!
//! Gestiona surtidores, acepta conexiones de vehículos y se comunica con el cluster
//! de cuentas de compañía para validar transacciones.

mod administrador_red;
mod estacion;
mod persistencia;
mod surtidor;

use actix::Actor;
use administrador_red::{AdministradorDeRed, ConfigurarDireccionesCluster, RegistrarSurtidor};
use common::parse_arg;
use estacion::EstacionDeServicio;
use log::info;
use network::Acceptor;
use std::env;
use std::io;
use surtidor::Surtidor;

/// Función principal del proceso estación.
#[actix_rt::main]
async fn main() {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!(
            "Proceso Estación de Servicio: Uso: {} <id> <cantidad_surtidores> <ip:puerto>",
            args[0]
        );
        std::process::exit(1);
    }

    let estacion_id: u32 = parse_arg(&args[1], "id");
    let cantidad_surtidores: u32 = parse_arg(&args[2], "cantidad_surtidores");
    let bind_addr = args[3].clone();

    info!("Proceso Estación de Servicio: Iniciando proceso");
    info!(
        "Configuración: ID={}, Surtidores={}, Dirección={}",
        estacion_id, cantidad_surtidores, &bind_addr
    );

    let mut estacion = EstacionDeServicio::new(estacion_id);

    let admin_red_addr = AdministradorDeRed::new(estacion_id).start();

    let mut surtidores_addrs = Vec::new();

    for id in 1..=cantidad_surtidores {
        let mut surtidor = Surtidor::new(id);
        surtidor.set_admin_red(admin_red_addr.clone());

        let addr = surtidor.start();

        estacion.agregar_surtidor(id, addr.clone());
        surtidores_addrs.push(addr.clone());

        admin_red_addr.do_send(RegistrarSurtidor {
            surtidor_id: id,
            addr,
        });
    }

    let estacion_addr = estacion.start();

    for addr in surtidores_addrs {
        addr.do_send(surtidor::SetEstacion(estacion_addr.clone()));
    }

    let _acceptor = Acceptor::new(bind_addr.clone(), estacion_addr.clone()).start();

    info!(
        "Estación {} escuchando en {} para vehículos",
        estacion_id, bind_addr
    );

    info!("Ingrese las direcciones IP:PUERTO de los nodos del cluster (una por línea, línea vacía para terminar):");

    let mut cluster_node_addrs = Vec::new();
    loop {
        let mut line = String::new();
        io::stdin()
            .read_line(&mut line)
            .expect("Failed to read line");
        let line = line.trim();
        if line.is_empty() {
            break;
        }
        cluster_node_addrs.push(line.to_string());
    }

    info!(
        "Enviando lista de {} nodos al Administrador de Red",
        cluster_node_addrs.len()
    );

    admin_red_addr.do_send(ConfigurarDireccionesCluster(cluster_node_addrs));

    tokio::signal::ctrl_c().await.unwrap();
    info!("Estación detenida");
}
