#!/bin/bash

# Start FastAPI backend on port 8000
cd backend
uvicorn main:app --reload --port 8000 --host 0.0.0.0 &
BACKEND_PID=$!

echo "Server running on http://localhost:8000"
echo ""
echo "Press Ctrl+C to stop the server"

# Trap Ctrl+C and kill the process
trap "kill $BACKEND_PID; exit" INT

# Wait for the process
wait
