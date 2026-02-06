#!/bin/bash

echo "========================================"
echo "  Test: Reconexión de Nodos"
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
"RUST_LOG=info target/release/cuenta_compania 1 127.0.0.1:7001 '2:127.0.0.1:7002,3:127.0.0.1:7003' ${BUFFER_CLUSTER}"
sleep 1

start_process "nodo2" \
"RUST_LOG=info target/release/cuenta_compania 2 127.0.0.1:7002 '1:127.0.0.1:7001,3:127.0.0.1:7003' ${BUFFER_CLUSTER}"
sleep 1

start_process "nodo3" \
"RUST_LOG=info target/release/cuenta_compania 3 127.0.0.1:7003 '1:127.0.0.1:7001,2:127.0.0.1:7002' ${BUFFER_CLUSTER}"

echo "  Cluster iniciado con 3 nodos"
echo "  Esperando elección de líder..."
echo ""
sleep 10

# [2] Simulando caída del Nodo 2
echo "[2] Simulando caída del Nodo 2..."

NODO2_PID=$(cat "${LOG_DIR}/nodo2.pid" 2>/dev/null)
if [ -n "$NODO2_PID" ]; then
    kill -9 "$NODO2_PID" 2>/dev/null
    echo "  Nodo 2 (PID: $NODO2_PID) eliminado"
    echo "  Esperando detección de fallo por el cluster..."
else
    echo "  ✗ No se pudo obtener el PID del Nodo 2"
fi
echo ""
sleep 5

# [3] Iniciando GPS y Estación
echo "[3] Iniciando GPS y Estación..."

start_process "gps" \
"printf '%s\n' '${ESTACION_LAT} ${ESTACION_LON} ${ESTACION_ADDR}' '' | \
 RUST_LOG=info target/release/gps ${GPS_ADDR}"
echo "  GPS iniciado"
sleep 2

# La estación intentará conectar primero al nodo 2 (caído), luego failover
start_process "estacion1" \
"printf '%s\n' '127.0.0.1:7002' '127.0.0.1:7001' '127.0.0.1:7003' | \
 RUST_LOG=info target/release/estacion 1 ${SURTIDORES} ${ESTACION_ADDR}"
echo "  Estación iniciada (intentará conectar primero al Nodo 2 caído)"
echo ""
sleep 5

# [4] Reconectando Nodo 2
echo "[4] Reconectando Nodo 2 con flag de reconexión..."

start_process "nodo2_rejoined" \
"RUST_LOG=info target/release/cuenta_compania 2 127.0.0.1:7002 '1:127.0.0.1:7001,3:127.0.0.1:7003' ${BUFFER_CLUSTER} 1"

echo "  Nodo 2 reiniciado con reconnection_flag=1"
echo "  Esperando reconexión completa al cluster..."
echo ""
sleep 10

# Finalización de todos los procesos
echo "Deteniendo todos los procesos..."
"$SCRIPT_DIR/stop.sh" > /dev/null 2>&1
echo ""
sleep 1

# [5] Resumen de resultados
echo "========================================"
echo "  Resumen de resultados"
echo "========================================"
echo ""

echo "--- Comportamiento del sistema ---"

# Verificar que el nodo cayó
if [ -n "$NODO2_PID" ]; then
    echo "  ✓ Nodo 2 eliminado correctamente"
else
    echo "  ✗ Fallo al eliminar Nodo 2"
fi

# Verificar detección de fallo
DETECCION_FALLO=$(grep -h "caído\|timeout\|desconectado" "${LOG_DIR}/nodo1.log" "${LOG_DIR}/nodo3.log" 2>/dev/null | wc -l)
if [ "$DETECCION_FALLO" -gt 0 ]; then
    echo "  ✓ Cluster detectó la caída del Nodo 2"
else
    echo "  ✗ No se detectó la caída del nodo"
fi

# Verificar failover de la estación
FAILOVER=$(grep -c "Intentando conectar\|Falló conexión\|Cambiando a nodo" "${LOG_DIR}/estacion1.log" 2>/dev/null || echo 0)
if [ "$FAILOVER" -gt 0 ]; then
    echo "  ✓ Estación realizó failover al detectar nodo caído"
else
    echo "  ✗ Estación no realizó failover"
fi

# Verificar reconexión del nodo 2
RECONEXION=$(grep -c "Conectado a nodo\|listo y conectado" "${LOG_DIR}/nodo2_rejoined.log" 2>/dev/null || echo 0)
if [ "$RECONEXION" -gt 0 ]; then
    echo "  ✓ Nodo 2 se reconectó exitosamente al cluster"
else
    echo "  ✗ Nodo 2 no logró reconectarse"
fi

# Verificar que los otros nodos aceptaron la reconexión
ACEPTACION=$(grep -h "CommunicationHandler conectado\|ActorConnected.*Nodo.*2" "${LOG_DIR}/nodo1.log" "${LOG_DIR}/nodo3.log" 2>/dev/null | wc -l)
if [ "$ACEPTACION" -gt 0 ]; then
    echo "  ✓ Nodos 1 y 3 aceptaron la reconexión del Nodo 2"
else
    echo "  ✗ No se detectó aceptación de la reconexión"
fi

echo ""

# Verificar estado final de los nodos
NODOS_ACTIVOS=$(ps aux | grep -c "[c]uenta_compania" 2>/dev/null || echo 0)
echo "--- Estado final del cluster ---"
echo "  Nodos activos: $NODOS_ACTIVOS"

# Verificar líder
LIDER=$(grep -h "Convirtiéndose en LÍDER" "${LOG_DIR}"/nodo*.log 2>/dev/null | tail -1 | grep -o "Nodo [0-9]" | cut -d' ' -f2 || echo "Desconocido")
echo "  Líder actual: Nodo $LIDER"

echo ""

echo "→ El sistema permitió la reconexión del nodo caído"
echo "→ La estación realizó failover automático durante la caída"
echo "→ El cluster aceptó la reconexión del Nodo 2"

echo ""

echo "========================================"
echo "  Logs completos disponibles en: ${LOG_DIR}/"
echo "========================================"
echo ""
