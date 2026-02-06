#!/bin/bash

echo "========================================"
echo "  Test: Flujo Completo de Carga"
echo "========================================"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Constantes
GPS_ADDR="127.0.0.1:8001"

BUFFER_CLUSTER="3"

ESTACION1_ADDR="127.0.0.1:8002"
ESTACION1_LAT="100"
ESTACION1_LON="200"

ESTACION2_ADDR="127.0.0.1:8003"
ESTACION2_LAT="300"
ESTACION2_LON="400"

ESTACION3_ADDR="127.0.0.1:8004"
ESTACION3_LAT="500"
ESTACION3_LON="600"

SURTIDORES="3"

LOG_DIR="logs"

# Función para iniciar procesos
start_process() {
    local name=$1
    local cmd=$2
    eval "$cmd" > "${LOG_DIR}/${name}.log" 2>&1 &
    echo $! > "${LOG_DIR}/${name}.pid"
    sleep 1
}

# Limpieza
echo "Limpiando entorno anterior..."
"$SCRIPT_DIR/stop.sh" > /dev/null 2>&1 || true
rm -rf ${LOG_DIR} *.log *.jsonl
mkdir -p ${LOG_DIR}
echo ""
sleep 1

# Compilación
echo "Compilando proyecto..."
cargo build --release
echo ""
sleep 1

# [1] Iniciando cluster CuentaCompania
echo "[1] Iniciando cluster CuentaCompania..."

nodes=()
while read -r id ip port limit; do
    [[ -z "$id" || "$id" =~ ^# ]] && continue
    nodes+=("$id $ip:$port $limit")
done < cluster_config.txt

for info in "${nodes[@]}"; do
    read -r id addr limit <<< "$info"

    peers=""
    for other in "${nodes[@]}"; do
        read -r oid oaddr _ <<< "$other"
        [[ "$oid" != "$id" ]] && peers+="${oid}:${oaddr},"
    done
    peers="${peers%,}"

    start_process "nodo${id}" \
        "RUST_LOG=info target/release/cuenta_compania ${id} ${addr} ${peers} ${BUFFER_CLUSTER}"

    sleep 1
done

echo "  Cluster iniciado con ${#nodes[@]} nodos y buffer ${BUFFER_CLUSTER}"
echo ""
sleep 3

# [2] Iniciando GPS
echo "[2] Iniciando GPS..."
start_process "gps" \
"printf '%s\n' \
 '${ESTACION1_LAT} ${ESTACION1_LON} ${ESTACION1_ADDR}' \
 '${ESTACION2_LAT} ${ESTACION2_LON} ${ESTACION2_ADDR}' \
 '${ESTACION3_LAT} ${ESTACION3_LON} ${ESTACION3_ADDR}' \
 '' | \
 RUST_LOG=info target/release/gps ${GPS_ADDR}"
echo "  GPS iniciado con 3 estaciones"
echo ""
sleep 3

# [3] Iniciando estaciones de servicio
echo "[3] Iniciando estaciones de servicio..."

addrs=()
for info in "${nodes[@]}"; do
    read -r _ addr _ <<< "$info"
    addrs+=("$addr")
done

rotate_list() {
    local -n arr=$1
    local shift_by=$2
    local n=${#arr[@]}
    local rotated=()
    for ((i=0; i<n; i++)); do
        rotated+=("${arr[( (i+shift_by) % n )]}")
    done
    printf "%s\n" "${rotated[@]}"
}

# Estación 1 → shift 1
start_process "estacion1" \
"rotate_list addrs 1 | \
 RUST_LOG=info target/release/estacion 1 ${SURTIDORES} ${ESTACION1_ADDR}"

sleep 1

# Estación 2 → shift 2
start_process "estacion2" \
"rotate_list addrs 2 | \
 RUST_LOG=info target/release/estacion 2 ${SURTIDORES} ${ESTACION2_ADDR}"

sleep 1

# Estación 3 → shift 3
start_process "estacion3" \
"rotate_list addrs 3 | \
 RUST_LOG=info target/release/estacion 3 ${SURTIDORES} ${ESTACION3_ADDR}"

echo "  3 estaciones de servicio iniciadas con ${SURTIDORES} surtidores cada una"
echo ""

# [4] Iniciando admins de compañías y definiendo límites
echo "[4] Iniciando admins de compañías y definiendo límites..."

# Admin Compañía 1 (Vehículos 101, 102, 103)
start_process "admin1" \
"(sleep 2 && \
 echo 'definir_limite_compania 50000.0' && \
 sleep 0.5 && \
 echo 'definir_limite_vehiculo 101 8000.0' && \
 sleep 0.3 && \
 echo 'definir_limite_vehiculo 102 12000.0' && \
 sleep 0.3 && \
 echo 'definir_limite_vehiculo 103 6000.0' && \
 sleep 0.5) | \
 RUST_LOG=info target/release/admin_compania 1 9101" &
echo $! > "${LOG_DIR}/admin1.pid"
sleep 0.5

# Admin Compañía 2 (Vehículos 201, 202, 203)
start_process "admin2" \
"(sleep 2 && \
 echo 'definir_limite_compania 75000.0' && \
 sleep 0.5 && \
 echo 'definir_limite_vehiculo 201 15000.0' && \
 sleep 0.3 && \
 echo 'definir_limite_vehiculo 202 9000.0' && \
 sleep 0.3 && \
 echo 'definir_limite_vehiculo 203 20000.0' && \
 sleep 0.5) | \
 RUST_LOG=info target/release/admin_compania 2 9102" &
echo $! > "${LOG_DIR}/admin2.pid"
sleep 0.5

# Admin Compañía 3 (Vehículos 301, 302, 303)
start_process "admin3" \
"(sleep 2 && \
 echo 'definir_limite_compania 60000.0' && \
 sleep 0.5 && \
 echo 'definir_limite_vehiculo 301 7000.0' && \
 sleep 0.3 && \
 echo 'definir_limite_vehiculo 302 11000.0' && \
 sleep 0.3 && \
 echo 'definir_limite_vehiculo 303 5000.0' && \
 sleep 0.5) | \
 RUST_LOG=info target/release/admin_compania 3 9103" &
echo $! > "${LOG_DIR}/admin3.pid"

echo "  Límites definidos:"
echo "    - Compañía 1: \$50,000 | Vehículo 101: \$8,000 | Vehículo 102: \$12,000 | Vehículo 103: \$6,000"
echo "    - Compañía 2: \$75,000 | Vehículo 201: \$15,000 | Vehículo 202: \$9,000 | Vehículo 203: \$20,000"
echo "    - Compañía 3: \$60,000 | Vehículo 301: \$7,000 | Vehículo 302: \$11,000 | Vehículo 303: \$5,000"
echo ""
sleep 4

# [5] Iniciando vehículos
echo "[5] Iniciando vehículos..."

# Compañía 1 - Vehículos en diferentes estaciones
# Vehículo 101 - Estación 1 - 50 litros = $5000 (límite vehículo $8000)
start_process "veh101" \
"printf '%s\n' '${GPS_ADDR}' '50.0' | \
 RUST_LOG=info target/release/vehiculo 101 1 ${ESTACION1_LAT} ${ESTACION1_LON}"
sleep 1

# Vehículo 102 - Estación 2 - 150 litros = $15000 (límite vehículo $12000)
start_process "veh102" \
"printf '%s\n' '${GPS_ADDR}' '150.0' | \
 RUST_LOG=info target/release/vehiculo 102 1 ${ESTACION2_LAT} ${ESTACION2_LON}"
sleep 1

# Vehículo 103 - Estación 3 - 40 litros = $4000 (límite vehículo $6000)
start_process "veh103" \
"printf '%s\n' '${GPS_ADDR}' '40.0' | \
 RUST_LOG=info target/release/vehiculo 103 1 ${ESTACION3_LAT} ${ESTACION3_LON}"
sleep 1

# Compañía 2 - Vehículos en diferentes estaciones
# Vehículo 201 - Estación 1 - 100 litros = $10000 (límite vehículo $15000)
start_process "veh201" \
"printf '%s\n' '${GPS_ADDR}' '100.0' | \
 RUST_LOG=info target/release/vehiculo 201 2 ${ESTACION1_LAT} ${ESTACION1_LON}"
sleep 1

# Vehículo 202 - Estación 2 - 120 litros = $12000 (límite vehículo $9000)
start_process "veh202" \
"printf '%s\n' '${GPS_ADDR}' '120.0' | \
 RUST_LOG=info target/release/vehiculo 202 2 ${ESTACION2_LAT} ${ESTACION2_LON}"
sleep 1

# Vehículo 203 - Estación 3 - 180 litros = $18000 (límite vehículo $20000)
start_process "veh203" \
"printf '%s\n' '${GPS_ADDR}' '180.0' | \
 RUST_LOG=info target/release/vehiculo 203 2 ${ESTACION3_LAT} ${ESTACION3_LON}"
sleep 1

# Compañía 3 - Vehículos en diferentes estaciones
# Vehículo 301 - Estación 1 - 65 litros = $6500 (límite vehículo $7000)
start_process "veh301" \
"printf '%s\n' '${GPS_ADDR}' '65.0' | \
 RUST_LOG=info target/release/vehiculo 301 3 ${ESTACION1_LAT} ${ESTACION1_LON}"
sleep 1

# Vehículo 302 - Estación 2 - 90 litros = $9000 (límite vehículo $11000)
start_process "veh302" \
"printf '%s\n' '${GPS_ADDR}' '90.0' | \
 RUST_LOG=info target/release/vehiculo 302 3 ${ESTACION2_LAT} ${ESTACION2_LON}"
sleep 1

# Vehículo 303 - Estación 3 - 60 litros = $6000 (límite vehículo $5000)
start_process "veh303" \
"printf '%s\n' '${GPS_ADDR}' '60.0' | \
 RUST_LOG=info target/release/vehiculo 303 3 ${ESTACION3_LAT} ${ESTACION3_LON}"

echo "  Cargas solicitadas:"
echo "    - Compañía 1: Vehículo 101 (50L/\$5,000) | Vehículo 102 (150L/\$15,000) | Vehículo 103 (40L/\$4,000)"
echo "    - Compañía 2: Vehículo 201 (100L/\$10,000) | Vehículo 202 (120L/\$12,000) | Vehículo 203 (180L/\$18,000)"
echo "    - Compañía 3: Vehículo 301 (65L/\$6,500) | Vehículo 302 (90L/\$9,000) | Vehículo 303 (60L/\$6,000)"
echo ""

trap "$SCRIPT_DIR/stop.sh" EXIT INT TERM

# Monitoreando resultados de las transacciones
echo "Monitoreando resultados de las transacciones (30 segundos)..."
echo ""

sleep 30

# [6] Consultando gastos finales
echo "[6] Consultando gastos de cada compañía..."

# Consultar saldo compañía 1
(sleep 1 && echo 'consultar_saldo_compania' && sleep 2 && echo 'salir') | \
 RUST_LOG=info target/release/admin_compania 1 9111 >> "${LOG_DIR}/admin1_final.log" 2>&1 &
sleep 0.5

# Consultar saldo compañía 2
(sleep 1 && echo 'consultar_saldo_compania' && sleep 2 && echo 'salir') | \
 RUST_LOG=info target/release/admin_compania 2 9112 >> "${LOG_DIR}/admin2_final.log" 2>&1 &
sleep 0.5

# Consultar saldo compañía 3
(sleep 1 && echo 'consultar_saldo_compania' && sleep 2 && echo 'salir') | \
 RUST_LOG=info target/release/admin_compania 3 9113 >> "${LOG_DIR}/admin3_final.log" 2>&1 &

echo "  Consultas por compañías realizadas."
sleep 5
echo ""

# Finalización de todos los procesos
echo "Deteniendo todos los procesos..."
"$SCRIPT_DIR/stop.sh" > /dev/null 2>&1
echo ""
sleep 1

# Resumen de resultados
echo "========================================"
echo "  Resumen de resultados"
echo "========================================"
echo ""

echo "--- Transacciones por vehículo ---"

declare -A expected_results=(
    [101]="ACEPTADA"
    [102]="RECHAZADA"
    [103]="ACEPTADA"
    [201]="ACEPTADA"
    [202]="RECHAZADA"
    [203]="ACEPTADA"
    [301]="ACEPTADA"
    [302]="ACEPTADA"
    [303]="RECHAZADA"
)

all_ok=true
for veh in 101 102 103 201 202 203 301 302 303; do
    log="${LOG_DIR}/veh${veh}.log"
    expected="${expected_results[$veh]}"
    if [ -f "$log" ]; then
        if grep -q "$expected" "$log" 2>/dev/null; then
            echo "  ✓ Vehículo $veh: $expected (correcto)"
        else
            actual=$(grep -E "ACEPTADA|RECHAZADA" "$log" 2>/dev/null | tail -1 || echo "Sin respuesta")
            echo "  ✗ Vehículo $veh: $actual (esperado: $expected)"
            all_ok=false
        fi
    else
        echo "  ✗ Vehículo $veh: Sin información (esperado: $expected)"
        all_ok=false
    fi
done

if [ "$all_ok" = true ]; then
    echo "  → Todas las transacciones procesadas correctamente"
fi
echo ""

echo "--- Saldos finales por compañía ---"

declare -A expected_saldos=(
    [1]="41000.00"
    [2]="47000.00"
    [3]="44500.00"
)

saldos_ok=true
for comp in 1 2 3; do
    log="${LOG_DIR}/admin${comp}_final.log"
    expected="${expected_saldos[$comp]}"
    if [ -f "$log" ]; then
        if grep -q "Saldo de compañía ${comp}: \$${expected}" "$log" 2>/dev/null; then
            echo "  ✓ Compañía $comp: Saldo \$$expected (correcto)"
        else
            actual=$(grep -E "Saldo de compañía" "$log" 2>/dev/null | tail -1 || echo "Sin respuesta")
            echo "  ✗ Compañía $comp: $actual (esperado: \$$expected)"
            saldos_ok=false
        fi
    else
        echo "  ✗ Compañía $comp: Sin información (esperado: \$$expected)"
        saldos_ok=false
    fi
done

if [ "$saldos_ok" = true ]; then
    echo "  → Todos los saldos correctos"
fi
echo ""

echo "========================================"
echo "  Logs completos disponibles en: ${LOG_DIR}/"
echo "========================================"
echo ""
