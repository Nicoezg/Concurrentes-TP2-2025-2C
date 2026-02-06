#!/bin/bash

echo "Deteniendo todos los procesos..."

# Detener usando PIDs guardados si existen
if [ -d "logs" ]; then
    for pidfile in logs/*.pid; do
        if [ -f "$pidfile" ]; then
            pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                echo "  Deteniendo proceso $pid ($(basename $pidfile))"
                kill "$pid" 2>/dev/null || true
            fi
            rm -f "$pidfile"
        fi
    done
fi

# Detener cualquier proceso que haya quedado
pkill -f "target/release/cuenta_compania" || true
pkill -f "target/release/admin_compania" || true
pkill -f "target/release/estacion" || true
pkill -f "target/release/vehiculo" || true
pkill -f "target/release/gps" || true

echo "Todos los procesos detenidos."
