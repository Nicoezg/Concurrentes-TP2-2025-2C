#!/bin/bash

echo "========================================"
echo "  Test: Caída de Nodo Común"
echo "========================================"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Constantes
GPS_ADDR="127.0.0.1:7000"
ESTACION_ADDR="127.0.0.1:8001"
ESTACION_LAT="100"
ESTACION_LON="200"
SURTIDORES="2"
BUFFER_CLUSTER="1"

LOG_DIR="logs"

# Función para iniciar procesos
start_process() {
    local name=$1
    local cmd=$2
    eval "$cmd" > "${LOG_DIR}/${name}.log" 2>&1 &
    local eval_pid=$!
    # Esperar un poco para que el proceso se inicie
    sleep 0.5
    # Encontrar el PID del proceso real
    local real_pid=$(pgrep -P $eval_pid)
    if [ -n "$real_pid" ]; then
        echo $real_pid > "${LOG_DIR}/${name}.pid"
    else
        echo $eval_pid > "${LOG_DIR}/${name}.pid"
    fi
    sleep 1
}

# Limpieza
echo "Limpiando entorno anterior..."
"$SCRIPT_DIR/stop.sh" > /dev/null 2>&1 || true
rm -rf ${LOG_DIR} *.log *.jsonl
mkdir -p ${LOG_DIR}
echo ""
sleep 1

# [1] Iniciando cluster de 3 nodos
echo "[1] Iniciando cluster de 3 nodos..."

start_process "nodo1" \
"RUST_LOG=info target/release/cuenta_compania 1 127.0.0.1:9001 '2:127.0.0.1:9002,3:127.0.0.1:9003' ${BUFFER_CLUSTER}"

start_process "nodo2" \
"RUST_LOG=info target/release/cuenta_compania 2 127.0.0.1:9002 '1:127.0.0.1:9001,3:127.0.0.1:9003' ${BUFFER_CLUSTER}"

start_process "nodo3" \
"RUST_LOG=info target/release/cuenta_compania 3 127.0.0.1:9003 '1:127.0.0.1:9001,2:127.0.0.1:9002' ${BUFFER_CLUSTER}"

echo "  Cluster iniciado con 3 nodos"
echo "  Nodo 1 (puerto 9001) - Común (ESTE NODO CAERÁ)"
echo "  Nodo 2 (puerto 9002) - Réplica"
echo "  Nodo 3 (puerto 9003) - Líder"
echo ""
sleep 3

# [2] Iniciando GPS
echo "[2] Iniciando GPS..."
start_process "gps" \
"printf '%s\n' '${ESTACION_LAT} ${ESTACION_LON} ${ESTACION_ADDR}' '' | \
 RUST_LOG=info target/release/gps ${GPS_ADDR}"
echo "  GPS iniciado con 1 estación"
echo ""
sleep 2

# [3] Iniciando estación de servicio
echo "[3] Iniciando estación de servicio..."
start_process "estacion1" \
"printf '%s\n' '127.0.0.1:9001' '127.0.0.1:9002' '127.0.0.1:9003' | \
 RUST_LOG=info target/release/estacion 1 ${SURTIDORES} ${ESTACION_ADDR}"
echo "  Estación iniciada con ${SURTIDORES} surtidores"
echo "  Conectada a los 3 nodos del cluster"
echo ""
sleep 4

# [4] Configurando límites de crédito
echo "[4] Configurando límites de crédito..."
start_process "admin1" \
"(sleep 2 && \
 echo 'definir_limite_compania 50000.0' && \
 sleep 0.5 && \
 echo 'definir_limite_vehiculo 101 10000.0' && \
 sleep 0.5 && \
 echo 'definir_limite_vehiculo 102 10000.0' && \
 sleep 0.5) | \
 RUST_LOG=info target/release/admin_compania 1 9101" &
echo $! > "${LOG_DIR}/admin1.pid"

echo "  Límites definidos:"
echo "    - Compañía 1: \$50,000"
echo "    - Vehículo 101: \$10,000"
echo "    - Vehículo 102: \$10,000"
echo ""
sleep 4

# [5] Primera transacción (ANTES de la caída)
echo "[5] Primera transacción (ANTES de la caída)..."

start_process "veh101" \
"printf '%s\n' '${GPS_ADDR}' '50.0' | \
 RUST_LOG=info target/release/vehiculo 101 1 ${ESTACION_LAT} ${ESTACION_LON}"

echo "  Vehículo 101: Solicitando 50L (\$5,000)"
echo ""
sleep 6

# Verificar resultado de la primera transacción
RESULTADO1=$(grep -E "ACEPTADA|RECHAZADA" "${LOG_DIR}/veh101.log" 2>/dev/null | tail -1)
if echo "$RESULTADO1" | grep -q "ACEPTADA"; then
    echo "  ✓ Primera transacción ACEPTADA (antes de la caída)"
else
    echo "  ✗ Primera transacción: $RESULTADO1"
fi
echo ""

# [6] Simulando caída del Nodo 1
echo "[6] Simulando caída del Nodo 1..."

NODO1_PID=$(cat "${LOG_DIR}/nodo1.pid" 2>/dev/null)
if [ -n "$NODO1_PID" ]; then
    kill -9 "$NODO1_PID" 2>/dev/null
    echo "  Nodo 1 (PID: $NODO1_PID) eliminado"
    echo "  La estación estaba conectada a este nodo"
    echo "  Esperando detección de fallo y reconexión..."
else
    echo "  ✗ No se pudo obtener el PID del Nodo 1"
fi
echo ""
sleep 15

# [7] Segunda transacción (DESPUÉS de la caída)
echo "[7] Segunda transacción (DESPUÉS de la caída)..."

start_process "veh102" \
"printf '%s\n' '${GPS_ADDR}' '50.0' | \
 RUST_LOG=info target/release/vehiculo 102 1 ${ESTACION_LAT} ${ESTACION_LON}"

echo "  Vehículo 102: Solicitando 50L (\$5,000)"
echo ""
sleep 6

# Verificar resultado de la segunda transacción
RESULTADO2=$(grep -E "ACEPTADA|RECHAZADA" "${LOG_DIR}/veh102.log" 2>/dev/null | tail -1)
if echo "$RESULTADO2" | grep -q "ACEPTADA"; then
    echo "  ✓ Segunda transacción ACEPTADA (después de la caída)"
else
    echo "  ✗ Segunda transacción: $RESULTADO2"
fi
echo ""

# Finalización de todos los procesos
echo "Deteniendo todos los procesos..."
"$SCRIPT_DIR/stop.sh" > /dev/null 2>&1
echo ""
sleep 1

# [8] Resumen de resultados
echo "========================================"
echo "  Resumen de resultados"
echo "========================================"
echo ""

echo "--- Comportamiento del sistema ---"

all_ok=true

# Verificar primera transacción
if echo "$RESULTADO1" | grep -q "ACEPTADA"; then
    echo "  ✓ Primera transacción: ACEPTADA (con cluster completo)"
else
    echo "  ✗ Primera transacción: Fallida"
    all_ok=false
fi

# Verificar que el nodo cayó
if [ -n "$NODO1_PID" ]; then
    echo "  ✓ Nodo 1 eliminado correctamente"
else
    echo "  ✗ Fallo al eliminar Nodo 1"
    all_ok=false
fi

# Verificar segunda transacción
if echo "$RESULTADO2" | grep -q "ACEPTADA"; then
    echo "  ✓ Segunda transacción: ACEPTADA (con 2 nodos operativos)"
else
    echo "  ✗ Segunda transacción: Fallida"
    all_ok=false
fi

echo ""

if [ "$all_ok" = true ]; then
    echo "→ El sistema tolera la caída de un nodo común"
    echo "→ Las transacciones se procesaron correctamente con 2/3 nodos"
    echo "→ La estación se reconectó exitosamente a otro nodo"
else
    echo "→ Se detectaron fallos en el comportamiento del sistema"
fi

echo ""

echo "--- Estadísticas del cluster ---"

TRANSACCIONES=$(grep -c "Transacción ACEPTADA\|Transacción RECHAZADA" "${LOG_DIR}/nodo3.log" 2>/dev/null || echo 0)
echo "  Transacciones procesadas por el líder: $TRANSACCIONES"

RECONEXIONES=$(grep -c "Intentando conectar\|Intento\|Error de conexión" "${LOG_DIR}/estacion1.log" 2>/dev/null || echo 0)
echo "  Intentos de reconexión de la estación: $RECONEXIONES"

echo ""

echo "========================================"
echo "  Logs completos disponibles en: ${LOG_DIR}/"
echo "========================================"
echo ""
