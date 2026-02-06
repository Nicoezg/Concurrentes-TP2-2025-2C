#!/bin/bash

echo "========================================"
echo "  Test: Falla del Nodo Líder"
echo "========================================"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Constantes
GPS_ADDR="127.0.0.1:8001"
ESTACION_ID="1"
ESTACION_ADDR="127.0.0.1:8002"
SURTIDORES="3"

COMPANIA_ID_1="1"
ADMIN_PORT_1="9101"
COMPANIA_ID_2="2"
ADMIN_PORT_2="9102"

VEH_ID_1="101"
VEH_ID_2="102"
VEH_ID_3="103"
VEH_ADDRESS_1="127.0.0.1:8201"
VEH_ADDRESS_2="127.0.0.1:8202"
VEH_ADDRESS_3="127.0.0.1:8203"
LAT="100"
LON="200"
CANT_CARGA="2.0"

CUENTA_ADDR_1="127.0.0.1:9001"
CUENTA_ADDR_2="127.0.0.1:9002"
CUENTA_ADDR_3="127.0.0.1:9003"
CUENTA_ID_1=1
CUENTA_ID_2=2
CUENTA_ID_3=3

BUFFER_SIZE=1

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

# [1] Iniciando cluster de 3 nodos
echo "[1] Iniciando cluster de 3 nodos..."
start_process "cuenta${CUENTA_ID_1}" \
    "RUST_LOG=info ./target/release/cuenta_compania ${CUENTA_ID_1} ${CUENTA_ADDR_1} '2:${CUENTA_ADDR_2},3:${CUENTA_ADDR_3}' ${BUFFER_SIZE}"

start_process "cuenta${CUENTA_ID_2}" \
    "RUST_LOG=info ./target/release/cuenta_compania ${CUENTA_ID_2} ${CUENTA_ADDR_2} '1:${CUENTA_ADDR_1},3:${CUENTA_ADDR_3}' ${BUFFER_SIZE}"

start_process "cuenta${CUENTA_ID_3}" \
    "RUST_LOG=info ./target/release/cuenta_compania ${CUENTA_ID_3} ${CUENTA_ADDR_3} '1:${CUENTA_ADDR_1},2:${CUENTA_ADDR_2}' ${BUFFER_SIZE}"

echo "  Cluster iniciado con 3 nodos (líder: nodo 3)"
echo ""
sleep 3

# [2] Iniciando estación de servicio
echo "[2] Iniciando estación de servicio..."
start_process "estacion${ESTACION_ID}" \
    "(echo ${CUENTA_ADDR_1}; echo ${CUENTA_ADDR_2}; echo ${CUENTA_ADDR_3}; echo '') | \
     RUST_LOG=info ./target/release/estacion ${ESTACION_ID} ${SURTIDORES} ${ESTACION_ADDR}"
echo "  Estación ${ESTACION_ID} iniciada con ${SURTIDORES} surtidores"
echo ""
sleep 3

# [3] Iniciando GPS
echo "[3] Iniciando GPS..."
start_process "gps" \
    "(echo '${LAT} ${LON} ${ESTACION_ADDR}'; echo '') | \
     RUST_LOG=info ./target/release/gps ${GPS_ADDR}"
echo "  GPS iniciado"
echo ""
sleep 2

# [4] Iniciando administradores de compañía
echo "[4] Iniciando administradores de compañía..."
start_process "admin1" \
    "(sleep 2 && echo 'definir_limite_compania 50000.0' && sleep 1 && \
      echo 'definir_limite_vehiculo 101 50000.0' && echo 'salir') | \
     RUST_LOG=info ./target/release/admin_compania ${COMPANIA_ID_1} ${ADMIN_PORT_1}"

start_process "admin2" \
    "(sleep 2 && echo 'definir_limite_compania 150000.0' && sleep 1 && \
      echo 'definir_limite_vehiculo 102 75000.0' && \
      echo 'definir_limite_vehiculo 103 15000.0' && sleep 40 && \
      echo 'consultar_saldo_compania' && echo 'consultar_saldo_vehiculo 102' && echo 'salir') | \
     RUST_LOG=info ./target/release/admin_compania ${COMPANIA_ID_2} ${ADMIN_PORT_2}"

echo "  Límites definidos:"
echo "    - Compañía 1: \$50,000 | Vehículo 101: \$50,000"
echo "    - Compañía 2: \$150,000 | Vehículo 102: \$75,000 | Vehículo 103: \$15,000"
echo ""
sleep 8

# [5] Iniciando vehículos iniciales
echo "[5] Iniciando vehículos iniciales..."
start_process "vehiculo${VEH_ID_1}" \
    "(echo ${GPS_ADDR}; sleep 2; echo ${CANT_CARGA}) | \
     RUST_LOG=info ./target/release/vehiculo ${VEH_ID_1} ${COMPANIA_ID_1} ${LAT} ${LON}"

start_process "vehiculo${VEH_ID_2}" \
    "(echo ${GPS_ADDR}; sleep 2; echo ${CANT_CARGA}) | \
     RUST_LOG=info ./target/release/vehiculo ${VEH_ID_2} ${COMPANIA_ID_2} ${LAT} ${LON}"

echo "  Vehículos 101 y 102 solicitando carga (${CANT_CARGA}L cada uno)"
echo ""
sleep 15

echo "[6] Simulando falla del nodo líder..."
pkill -f "cuenta_compania ${CUENTA_ID_3}"
echo "  Nodo líder (nodo 3) detenido"
echo ""
sleep 8

echo "[7] Verificando elección de nuevo líder..."
sleep 2
echo "  Esperando estabilización del cluster"
echo ""

# [8] Iniciando vehículo después de la falla
echo "[8] Iniciando vehículo después de la falla del líder..."
start_process "vehiculo${VEH_ID_3}" \
    "(echo ${GPS_ADDR}; sleep 2; echo ${CANT_CARGA}) | \
     RUST_LOG=info ./target/release/vehiculo ${VEH_ID_3} ${COMPANIA_ID_2} ${LAT} ${LON}"

echo "  Vehículo 103 solicitando carga (${CANT_CARGA}L)"
echo ""
sleep 15

# Deteniendo procesos
echo "Deteniendo todos los procesos..."
"$SCRIPT_DIR/stop.sh" > /dev/null 2>&1
echo ""
sleep 1

# Resumen de resultados
echo "========================================"
echo "  Resumen de resultados"
echo "========================================"
echo ""

echo "--- Verificación de límites configurados ---"
if grep -i "límite" ${LOG_DIR}/cuenta${CUENTA_ID_3}.log > /dev/null 2>&1; then
    echo "  ✓ Límites configurados correctamente en el líder inicial"
else
    echo "  ✗ No se encontraron límites configurados"
fi
echo ""

echo "--- Transacciones antes de la falla ---"
if grep -i "APROBADA" ${LOG_DIR}/cuenta${CUENTA_ID_1}.log > /dev/null 2>&1; then
    echo "  ✓ Vehículos 101 y 102 cargaron correctamente"
else
    echo "  ✗ Error en transacciones iniciales"
fi
echo ""

echo "--- Detección de falla del líder ---"
if grep -i "Líder ${CUENTA_ID_3} caído" ${LOG_DIR}/cuenta${CUENTA_ID_1}.log > /dev/null 2>&1 && \
   grep -i "Líder ${CUENTA_ID_3} caído" ${LOG_DIR}/cuenta${CUENTA_ID_2}.log > /dev/null 2>&1; then
    echo "  ✓ Nodos detectaron la falla del líder"
else
    echo "  ✗ Nodos no detectaron la falla"
fi
echo ""

echo "--- Elección de nuevo líder ---"
if grep -i "Nodo 2: Convirtiéndose en LÍDER" ${LOG_DIR}/cuenta${CUENTA_ID_2}.log > /dev/null 2>&1; then
    echo "  ✓ Nuevo líder elegido correctamente (nodo 2)"
else
    echo "  ✗ No se eligió un nuevo líder"
fi
echo ""

echo "--- Reconocimiento de réplicas ---"
if grep -i -E "réplica|replica" ${LOG_DIR}/cuenta${CUENTA_ID_2}.log > /dev/null 2>&1; then
    echo "  ✓ Nuevo líder reconoció las réplicas"
else
    echo "  ✗ Nuevo líder no reconoció las réplicas"
fi
echo ""

echo "--- Transacción después de la falla ---"
if grep -i "APROBADA" ${LOG_DIR}/cuenta${CUENTA_ID_1}.log > /dev/null 2>&1; then
    echo "  ✓ Vehículo 103 cargó correctamente"
else
    echo "  ✗ Error en transacción del vehículo 103"
fi
echo ""

echo "--- Verificación de saldos ---"
if grep -i "Saldo de compañía ${CUENTA_ID_2}: \$149600.00" ${LOG_DIR}/admin2.log > /dev/null 2>&1 && \
   grep -i "Saldo de vehículo ${VEH_ID_2}: \$74800.00" ${LOG_DIR}/admin2.log > /dev/null 2>&1; then
    echo "  ✓ Saldos correctos después de operaciones"
else
    echo "  ✗ Saldos incorrectos"
fi
echo ""

echo "========================================"
echo "  Logs completos disponibles en: ${LOG_DIR}/"
echo "========================================"
echo ""