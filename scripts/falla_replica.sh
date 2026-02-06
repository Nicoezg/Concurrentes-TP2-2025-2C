#!/bin/bash

echo "========================================"
echo "  Test: Caída de Nodo Réplica"
echo "========================================"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Constantes
GPS_ADDR="127.0.0.1:7000"

BUFFER_CLUSTER="1"

ESTACION1_ADDR="127.0.0.1:8001"
ESTACION1_LAT="100"
ESTACION1_LON="200"

ESTACION2_ADDR="127.0.0.1:8002"
ESTACION2_LAT="300"
ESTACION2_LON="400"

ESTACION3_ADDR="127.0.0.1:8003"
ESTACION3_LAT="500"
ESTACION3_LON="600"

SURTIDORES="2"

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

# [1] Iniciando cluster de 5 nodos
echo "[1] Iniciando cluster de 5 nodos..."

start_process "nodo1" \
"RUST_LOG=info target/release/cuenta_compania 1 127.0.0.1:9001 '2:127.0.0.1:9002,3:127.0.0.1:9003,4:127.0.0.1:9004,5:127.0.0.1:9005' ${BUFFER_CLUSTER}"

start_process "nodo2" \
"RUST_LOG=info target/release/cuenta_compania 2 127.0.0.1:9002 '1:127.0.0.1:9001,3:127.0.0.1:9003,4:127.0.0.1:9004,5:127.0.0.1:9005' ${BUFFER_CLUSTER}"

start_process "nodo3" \
"RUST_LOG=info target/release/cuenta_compania 3 127.0.0.1:9003 '1:127.0.0.1:9001,2:127.0.0.1:9002,4:127.0.0.1:9004,5:127.0.0.1:9005' ${BUFFER_CLUSTER}"

start_process "nodo4" \
"RUST_LOG=info target/release/cuenta_compania 4 127.0.0.1:9004 '1:127.0.0.1:9001,2:127.0.0.1:9002,3:127.0.0.1:9003,5:127.0.0.1:9005' ${BUFFER_CLUSTER}"

start_process "nodo5" \
"RUST_LOG=info target/release/cuenta_compania 5 127.0.0.1:9005 '1:127.0.0.1:9001,2:127.0.0.1:9002,3:127.0.0.1:9003,4:127.0.0.1:9004' ${BUFFER_CLUSTER}"

echo "  Cluster iniciado con 5 nodos y buffer ${BUFFER_CLUSTER}"
echo "  Nodo 4 será la réplica que caerá"
echo "  Nodo 5 será el líder"
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
sleep 2

# [3] Iniciando estaciones de servicio
echo "[3] Iniciando estaciones de servicio..."

echo "  Esperando estabilización del cluster..."
sleep 10

start_process "estacion1" \
"printf '%s\n' '127.0.0.1:9001' '127.0.0.1:9002' '127.0.0.1:9003' '127.0.0.1:9004' '127.0.0.1:9005' | \
 RUST_LOG=info target/release/estacion 1 ${SURTIDORES} ${ESTACION1_ADDR}"
sleep 3

start_process "estacion2" \
"printf '%s\n' '127.0.0.1:9001' '127.0.0.1:9002' '127.0.0.1:9003' '127.0.0.1:9004' '127.0.0.1:9005' | \
 RUST_LOG=info target/release/estacion 2 ${SURTIDORES} ${ESTACION2_ADDR}"
sleep 3

start_process "estacion3" \
"printf '%s\n' '127.0.0.1:9001' '127.0.0.1:9002' '127.0.0.1:9003' '127.0.0.1:9004' '127.0.0.1:9005' | \
 RUST_LOG=info target/release/estacion 3 ${SURTIDORES} ${ESTACION3_ADDR}"

echo "  3 estaciones de servicio iniciadas con ${SURTIDORES} surtidores cada una"
echo ""
sleep 4

# [4] Iniciando admins de compañías y definiendo límites
echo "[4] Iniciando admins de compañías y definiendo límites..."

start_process "admin1" \
"(sleep 2 && \
 echo 'definir_limite_compania 50000.0' && \
 sleep 0.5 && \
 echo 'definir_limite_vehiculo 101 10000.0' && \
 sleep 0.3 && \
 echo 'definir_limite_vehiculo 102 10000.0' && \
 sleep 0.5) | \
 RUST_LOG=info target/release/admin_compania 1 9101" &
echo $! > "${LOG_DIR}/admin1.pid"
sleep 0.5

start_process "admin2" \
"(sleep 2 && \
 echo 'definir_limite_compania 30000.0' && \
 sleep 0.5 && \
 echo 'definir_limite_vehiculo 201 8000.0' && \
 sleep 0.3 && \
 echo 'definir_limite_vehiculo 202 9000.0' && \
 sleep 0.5) | \
 RUST_LOG=info target/release/admin_compania 2 9102" &
echo $! > "${LOG_DIR}/admin2.pid"
sleep 0.5

start_process "admin3" \
"(sleep 2 && \
 echo 'definir_limite_compania 40000.0' && \
 sleep 0.5 && \
 echo 'definir_limite_vehiculo 301 12000.0' && \
 sleep 0.3 && \
 echo 'definir_limite_vehiculo 302 11000.0' && \
 sleep 0.5) | \
 RUST_LOG=info target/release/admin_compania 3 9103" &
echo $! > "${LOG_DIR}/admin3.pid"

echo "  Límites definidos:"
echo "    - Compañía 1: \$50,000 | Vehículo 101: \$10,000 | Vehículo 102: \$10,000"
echo "    - Compañía 2: \$30,000 | Vehículo 201: \$8,000 | Vehículo 202: \$9,000"
echo "    - Compañía 3: \$40,000 | Vehículo 301: \$12,000 | Vehículo 302: \$11,000"
echo ""
sleep 4

# [5] Iniciando vehículos (ANTES de la caída)
echo "[5] Iniciando vehículos (ANTES de la caída)..."

start_process "veh101" \
"printf '%s\n' '${GPS_ADDR}' '50.0' | \
 RUST_LOG=info target/release/vehiculo 101 1 ${ESTACION1_LAT} ${ESTACION1_LON}"
sleep 1

start_process "veh201" \
"printf '%s\n' '${GPS_ADDR}' '40.0' | \
 RUST_LOG=info target/release/vehiculo 201 2 ${ESTACION2_LAT} ${ESTACION2_LON}"
sleep 1

start_process "veh301" \
"printf '%s\n' '${GPS_ADDR}' '60.0' | \
 RUST_LOG=info target/release/vehiculo 301 3 ${ESTACION3_LAT} ${ESTACION3_LON}"

echo "  Cargas solicitadas:"
echo "    - Vehículo 101 (Compañía 1): 50L (\$5,000) en Estación 1"
echo "    - Vehículo 201 (Compañía 2): 40L (\$4,000) en Estación 2"
echo "    - Vehículo 301 (Compañía 3): 60L (\$6,000) en Estación 3"
echo ""
sleep 6

# [6] Simulando caída del Nodo 4 (réplica)
echo "[6] Simulando caída del Nodo 4 (réplica)..."

# Usar pkill para asegurar que matamos el proceso real de Rust
NODO4_KILLED=$(pkill -9 -f "cuenta_compania 4 127.0.0.1:9004" && echo "yes" || echo "no")
if [ "$NODO4_KILLED" = "yes" ]; then
    echo "  Nodo 4 eliminado correctamente"
    echo "  El líder debe detectar la caída y elegir una nueva réplica"
    echo "  Esperando detección de fallo y reconfiguración..."
else
    echo "  ✗ No se pudo eliminar el Nodo 4"
fi
echo ""
sleep 15

# [7] Iniciando vehículos (DESPUÉS de la caída)
echo "[7] Iniciando vehículos (DESPUÉS de la caída)..."

start_process "veh102" \
"printf '%s\n' '${GPS_ADDR}' '35.0' | \
 RUST_LOG=info target/release/vehiculo 102 1 ${ESTACION1_LAT} ${ESTACION1_LON}"
sleep 1

start_process "veh202" \
"printf '%s\n' '${GPS_ADDR}' '45.0' | \
 RUST_LOG=info target/release/vehiculo 202 2 ${ESTACION2_LAT} ${ESTACION2_LON}"
sleep 1

start_process "veh302" \
"printf '%s\n' '${GPS_ADDR}' '55.0' | \
 RUST_LOG=info target/release/vehiculo 302 3 ${ESTACION3_LAT} ${ESTACION3_LON}"

echo "  Cargas solicitadas:"
echo "    - Vehículo 102 (Compañía 1): 35L (\$3,500) en Estación 1"
echo "    - Vehículo 202 (Compañía 2): 45L (\$4,500) en Estación 2"
echo "    - Vehículo 302 (Compañía 3): 55L (\$5,500) en Estación 3"
echo ""
sleep 6

# [8] Consultando saldos finales
echo "[8] Consultando saldos de cada compañía..."

(sleep 1 && echo 'consultar_saldo_compania' && sleep 2 && echo 'salir') | \
 RUST_LOG=info target/release/admin_compania 1 9111 >> "${LOG_DIR}/admin1_final.log" 2>&1 &
sleep 0.5

(sleep 1 && echo 'consultar_saldo_compania' && sleep 2 && echo 'salir') | \
 RUST_LOG=info target/release/admin_compania 2 9112 >> "${LOG_DIR}/admin2_final.log" 2>&1 &
sleep 0.5

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

echo "--- Transacciones antes de la caída ---"

declare -A expected_before=(
    [101]="ACEPTADA"
    [201]="ACEPTADA"
    [301]="ACEPTADA"
)

all_ok_before=true
for veh in 101 201 301; do
    log="${LOG_DIR}/veh${veh}.log"
    expected="${expected_before[$veh]}"
    if [ -f "$log" ]; then
        if grep -q "$expected" "$log" 2>/dev/null; then
            echo "  ✓ Vehículo $veh: $expected"
        else
            actual=$(grep -E "ACEPTADA|RECHAZADA" "$log" 2>/dev/null | tail -1 || echo "Sin respuesta")
            echo "  ✗ Vehículo $veh: $actual (esperado: $expected)"
            all_ok_before=false
        fi
    else
        echo "  ✗ Vehículo $veh: Sin información"
        all_ok_before=false
    fi
done

if [ "$all_ok_before" = true ]; then
    echo "  → Todas las transacciones previas a la caída correctas"
fi
echo ""

echo "--- Transacciones después de la caída ---"

declare -A expected_after=(
    [102]="ACEPTADA"
    [202]="ACEPTADA"
    [302]="ACEPTADA"
)

all_ok_after=true
for veh in 102 202 302; do
    log="${LOG_DIR}/veh${veh}.log"
    expected="${expected_after[$veh]}"
    if [ -f "$log" ]; then
        if grep -q "$expected" "$log" 2>/dev/null; then
            echo "  ✓ Vehículo $veh: $expected"
        else
            actual=$(grep -E "ACEPTADA|RECHAZADA" "$log" 2>/dev/null | tail -1 || echo "Sin respuesta")
            echo "  ✗ Vehículo $veh: $actual (esperado: $expected)"
            all_ok_after=false
        fi
    else
        echo "  ✗ Vehículo $veh: Sin información"
        all_ok_after=false
    fi
done

if [ "$all_ok_after" = true ]; then
    echo "  → Todas las transacciones posteriores a la caída correctas"
fi
echo ""

echo "--- Comportamiento del sistema ---"

# Verificar que el nodo cayó
if [ "$NODO4_KILLED" = "yes" ]; then
    echo "  ✓ Nodo 4 (réplica) eliminado correctamente"
else
    echo "  ✗ Fallo al eliminar Nodo 4"
fi

# Verificar detección de nueva réplica
NUEVA_REPLICA=$(grep -h "nueva réplica\|Nueva RÉPLICA\|Eligiendo réplica" "${LOG_DIR}"/nodo*.log 2>/dev/null | wc -l)
if [ "$NUEVA_REPLICA" -gt 0 ]; then
    echo "  ✓ Líder detectó la caída y eligió nueva réplica"
else
    echo "  ✗ No se detectó elección de nueva réplica"
fi

echo ""

if [ "$all_ok_before" = true ] && [ "$all_ok_after" = true ]; then
    echo "→ El sistema tolera la caída de la réplica"
    echo "→ Las transacciones continuaron correctamente con 4/5 nodos"
    echo "→ El líder eligió automáticamente una nueva réplica"
else
    echo "→ Se detectaron fallos en el comportamiento del sistema"
fi

echo ""

echo "--- Estadísticas del cluster ---"

TRANSACCIONES_LIDER=$(grep -c "Transacción ACEPTADA\|Transacción RECHAZADA" "${LOG_DIR}/nodo5.log" 2>/dev/null || echo 0)
echo "  Transacciones procesadas por el líder: $TRANSACCIONES_LIDER"

echo ""

echo "========================================"
echo "  Logs completos disponibles en: ${LOG_DIR}/"
echo "========================================"
echo ""
