#!/bin/bash

echo "========================================"
echo "  Test: Estación sin Conexión Inicial"
echo "========================================"
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Constantes
GPS_ADDR="127.0.0.1:8001"
ESTACION_ID="1"
ESTACION_ADDR="127.0.0.1:8002"
SURTIDORES="2"

ADMIN_ID="1"
ADMIN_PORT="9101"

VEH_ID="101"
COMPANIA_ID="1"
LAT="100"
LON="200"
CANT_CARGA="2.0"

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

# [1] Iniciando GPS
echo "[1] Iniciando GPS..."
start_process "gps" \
"echo -e \"${LAT} ${LON} ${ESTACION_ADDR}\n\" | RUST_LOG=info target/release/gps ${GPS_ADDR}"
echo "  GPS iniciado"
echo ""
sleep 2

# [2] Iniciando estación SIN nodos cuenta compañía
echo "[2] Iniciando estación SIN nodos cuenta compañía..."
start_process "estacion" \
"echo -e \"\" | RUST_LOG=info target/release/estacion ${ESTACION_ID} ${SURTIDORES} ${ESTACION_ADDR}"
echo "  Estación iniciada sin conexión a cluster"
echo ""
sleep 3

# [3] Iniciando vehículo (sin cluster disponible)
echo "[3] Iniciando vehículo (sin cluster disponible)..."

VEH_FIFO="${LOG_DIR}/veh_${VEH_ID}.fifo"
rm -f "${VEH_FIFO}" || true
mkfifo "${VEH_FIFO}"

nohup sh -c "while true; do cat \"${VEH_FIFO}\"; sleep 0.1; done | \
 RUST_LOG=info target/release/vehiculo ${VEH_ID} ${COMPANIA_ID} ${LAT} ${LON}" \
 > "${LOG_DIR}/vehiculo_${VEH_ID}.log" 2>&1 &

echo $! > "${LOG_DIR}/vehiculo_${VEH_ID}.pid"

printf '%s\n' "${GPS_ADDR}" "${CANT_CARGA}" > "${VEH_FIFO}"

echo "  Vehículo ${VEH_ID} solicitando carga: ${CANT_CARGA}L (\$200) - sin cluster disponible"
echo ""
sleep 15

# [4] Iniciando nodos del clúster
echo "[4] Iniciando nodos del clúster..."

nodes=()
if [ ! -f "cluster_config.txt" ]; then
    echo "Error: no existe cluster_config.txt"
    exit 1
fi

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
        "RUST_LOG=info target/release/cuenta_compania ${id} ${addr} ${peers} 1"
done

echo "  Clúster iniciado con ${#nodes[@]} nodos"
echo ""
sleep 5

# [5] Iniciando admin y definiendo límites
echo "[5] Iniciando admin y definiendo límites..."

start_process "admin" \
"(sleep 2 && \
 echo 'definir_limite_compania 100000.0' && \
 sleep 0.5 && \
 echo 'definir_limite_vehiculo 101 10000.0' && \
 sleep 0.5 && \
 echo 'consultar_saldo_compania' && \
 sleep 0.5 && \
 echo 'salir') | \
 RUST_LOG=info target/release/admin_compania ${ADMIN_ID} ${ADMIN_PORT}"

echo "  Límites definidos:"
echo "    - Compañía ${COMPANIA_ID}: \$100,000"
echo "    - Vehículo ${VEH_ID}: \$10,000"
echo ""
sleep 3

# [6] Reiniciando estación CON nodos
echo "[6] Reiniciando estación CON nodos..."

pkill -f "target/release/estacion" || true
sleep 2

cluster_list=""
for info in "${nodes[@]}"; do
    read -r _ addr _ <<< "$info"
    cluster_list+="${addr}\n"
done

start_process "estacion" \
"echo -e \"${cluster_list}\" | \
 RUST_LOG=info target/release/estacion ${ESTACION_ID} ${SURTIDORES} ${ESTACION_ADDR}"

echo "  Estación reiniciada con conexión a cluster"
echo ""
sleep 5

# [7] Segunda operación de carga del vehículo
echo "[7] Vehículo realiza segunda operación de carga..."

SEGUNDA_CARGA_ENVIADA=0
for _ in {1..30}; do
    if tail -1 "${LOG_DIR}/vehiculo_${VEH_ID}.log" | grep -qE "ACEPTADA|RECHAZADA"; then
        if [ "$SEGUNDA_CARGA_ENVIADA" -eq 0 ]; then
            sleep 2
            printf '%s\n' "${CANT_CARGA}" > "${VEH_FIFO}"
            SEGUNDA_CARGA_ENVIADA=1
        fi
        break
    fi
    sleep 1
done

echo "  Segunda carga solicitada: ${CANT_CARGA}L (\$200) - con cluster disponible"
echo ""

# [8] Monitoreando resultado
echo "[8] Monitoreando resultado (30 segundos)..."

for _ in {1..30}; do
    log="${LOG_DIR}/vehiculo_${VEH_ID}.log"
    if [ -f "$log" ]; then
        if tail -1 "$log" | grep -q "ACEPTADA"; then
            RESULT="ACEPTADA"
            break
        fi
        if tail -1 "$log" | grep -q "RECHAZADA"; then
            RESULT="RECHAZADA"
            break
        fi
    fi
    sleep 2
done

[[ -z "$RESULT" ]] && RESULT="SIN RESPUESTA"

echo "  Resultado: $RESULT"
echo ""

# Deteniendo procesos
echo "Deteniendo todos los procesos..."
"$SCRIPT_DIR/stop.sh" > /dev/null 2>&1
echo ""
sleep 1

# Resumen de resultados
echo "======================================="
echo "  Resumen de resultados"
echo "======================================="
echo ""

echo "--- Primera transacción (sin cluster) ---"
if grep -q "Timeout" "${LOG_DIR}/vehiculo_${VEH_ID}.log" 2>/dev/null; then
    echo "  ✓ Vehículo ${VEH_ID}: Timeout detectado (esperado sin cluster)"
else
    echo "  ✗ Vehículo ${VEH_ID}: No se detectó timeout"
fi
echo ""

echo "--- Reconexión de estación ---"
if grep -qi "conectada\|conectado" "${LOG_DIR}/estacion.log" 2>/dev/null; then
    echo "  ✓ Estación reconectada al cluster correctamente"
else
    echo "  ✗ Estación no reconectó al cluster"
fi
echo ""

echo "--- Segunda transacción (con cluster) ---"
if grep -q "ACEPTADA" "${LOG_DIR}/vehiculo_${VEH_ID}.log" 2>/dev/null; then
    echo "  ✓ Vehículo ${VEH_ID}: Transacción ACEPTADA (correcto)"
elif grep -q "RECHAZADA" "${LOG_DIR}/vehiculo_${VEH_ID}.log" 2>/dev/null; then
    echo "  ✗ Vehículo ${VEH_ID}: Transacción RECHAZADA (inesperado)"
else
    echo "  ✗ Vehículo ${VEH_ID}: Sin respuesta"
fi
echo ""

echo "========================================"
echo "  Logs completos disponibles en: ${LOG_DIR}/"
echo "========================================"
echo ""
