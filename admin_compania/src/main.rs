//! Proceso principal del administrador de compañía.
//!
//! Proporciona una CLI para que el administrador de una compañía consulte saldos,
//! defina límites de gasto y solicite reportes al cluster de cuentas.

mod admin_actor_impls;
mod admin_business_logic;
mod admin_message_handlers;
mod admin_types;

use actix::Actor;
use admin_types::*;
use common::parse_arg;
use log::info;
use std::env;
use std::io;
use tokio::time::{sleep, Duration};

#[actix_rt::main]
async fn main() {
    env_logger::init();
    info!("Iniciando proceso AdministradorDeCompania");

    let args: Vec<String> = env::args().collect();

    if args.len() != 3 {
        eprintln!("Uso: {} <compania_id> <bind_port>", args[0]);
        eprintln!("Ejemplos:");
        eprintln!("  {} 1 9100", args[0]);
        std::process::exit(1);
    }

    let compania_id: u32 = parse_arg(&args[1], "compania_id");
    let bind_port: u16 = parse_arg(&args[2], "bind_port");

    let bind_addr = format!("127.0.0.1:{}", bind_port);

    // Crear información del cluster (esto podría leerse de un archivo de configuración en el futuro)
    let cluster_nodes = vec![
        common::NodeInfo {
            id: 1,
            address: "127.0.0.1:9001".to_string(),
        },
        common::NodeInfo {
            id: 2,
            address: "127.0.0.1:9002".to_string(),
        },
        common::NodeInfo {
            id: 3,
            address: "127.0.0.1:9003".to_string(),
        },
    ];

    // Crear el administrador
    let admin = AdministradorDeCompania::new(compania_id, cluster_nodes);
    let admin_addr = admin.start();

    info!(
        "AdministradorDeCompania {} iniciado en {}",
        compania_id, bind_addr
    );

    info!(
        "Admin {}: El actor se conectará autónomamente al líder del cluster",
        compania_id
    );
    sleep(Duration::from_millis(3000)).await;

    info!("CLI de AdministradorCompania Listo. Ingresa comandos o 'salir' para finalizar:");

    let admin_clone = admin_addr.clone();
    tokio::task::spawn_blocking(move || loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        let input = input.trim();
        if input.is_empty() || input == "salir" || input == "6" {
            println!("CLI finalizado. Saliendo...");
            break;
        }

        let parts: Vec<&str> = input.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }

        match parts[0] {
            "definir_limite_compania" | "1" => {
                if parts.len() != 2 {
                    println!("Error: uso correcto -> definir_limite_compania <monto>");
                    continue;
                }
                if let Ok(limite) = parts[1].parse::<f64>() {
                    admin_clone.do_send(ComandoDefinirLimiteCompania { limite });
                    println!(
                        "Comando enviado: definir límite de compañía a ${:.2}",
                        limite
                    );
                } else {
                    println!("Error: monto inválido");
                }
            }
            "definir_limite_vehiculo" | "2" => {
                if parts.len() != 3 {
                    println!(
                        "Error: uso correcto -> definir_limite_vehiculo <vehiculo_id> <monto>"
                    );
                    continue;
                }
                if let (Ok(vehiculo_id), Ok(limite)) =
                    (parts[1].parse::<u32>(), parts[2].parse::<f64>())
                {
                    admin_clone.do_send(ComandoDefinirLimiteVehiculo {
                        vehiculo_id,
                        limite,
                    });
                    println!(
                        "Comando enviado: definir límite del vehículo {} a ${:.2}",
                        vehiculo_id, limite
                    );
                } else {
                    println!("Error: parámetros inválidos");
                }
            }
            "consultar_saldo_compania" | "3" => {
                admin_clone.do_send(ComandoConsultarSaldoCompania);
                println!("Comando enviado: consultar saldo de compañía");
            }
            "consultar_saldo_vehiculo" | "4" => {
                if parts.len() != 2 {
                    println!("Error: uso correcto -> consultar_saldo_vehiculo <vehiculo_id>");
                    continue;
                }
                if let Ok(vehiculo_id) = parts[1].parse::<u32>() {
                    admin_clone.do_send(ComandoConsultarSaldoVehiculo { vehiculo_id });
                    println!(
                        "Comando enviado: consultar saldo del vehículo {}",
                        vehiculo_id
                    );
                } else {
                    println!("Error: vehiculo_id inválido");
                }
            }
            "solicitar_reporte_mensual" | "5" => {
                admin_clone.do_send(ComandoSolicitarReporteMensual);
                println!("Comando enviado: solicitar reporte mensual");
            }
            _ => {
                println!("Comando desconocido: {}", parts[0]);
            }
        }
    });

    tokio::signal::ctrl_c().await.unwrap();
    info!("AdministradorDeCompania detenido");
}
