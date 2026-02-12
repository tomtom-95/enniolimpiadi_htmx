#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

export PROJECT_ROOT="$SCRIPT_DIR"
export DATABASE_PATH="$SCRIPT_DIR/backend/olympiad.db"
export SCHEMA_PATH="$SCRIPT_DIR/backend/schema.sql"

# Create virtual environment if it doesn't exist
if [ ! -d "$SCRIPT_DIR/venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$SCRIPT_DIR/venv"
fi

# Activate virtual environment
source "$SCRIPT_DIR/venv/bin/activate"

# Install requirements
echo "Installing requirements..."
pip install -r "$SCRIPT_DIR/backend/requirements.txt" -q

cd "$SCRIPT_DIR/backend"

case "$1" in
    test)
        # Use test database
        export DATABASE_PATH="$SCRIPT_DIR/backend/testdb.db"

        pytest
        ;;
    run)
        # Start FastAPI backend
        python -m src.main &
        BACKEND_PID=$!

        # Wait for the process
        wait
        ;;
    *)
        echo "Usage: $0 {run|test|stop}"
        exit 1
        ;;
esac
