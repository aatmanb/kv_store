#!/bin/bash

# Function to clean up processes on exit
cleanup() {
    echo "Stopping the server..."
    kill "$SERVER_PID" 2>/dev/null
    wait "$SERVER_PID" 2>/dev/null
    echo "Server stopped."
    exit
}

# Trap SIGINT (Ctrl+C) and SIGTERM (kill) signals
trap cleanup SIGINT SIGTERM

# Check if the number of clients and output directory are provided as arguments
if [ $# -ne 2 ]; then
  echo "Usage: $0 <number_of_clients> <output_directory>"
  exit 1
fi

NUM_CLIENTS=$1
OUTPUT_DIR=$2

# Check if the output directory exists, if not create it
if [ ! -d "$OUTPUT_DIR" ]; then
  mkdir -p "$OUTPUT_DIR"
fi

# Navigate to the directory containing the binaries (two levels down)
BIN_DIR="./cmake/build"  # Adjust this path based on your actual structure

# Start the server in the background and redirect output to server.log
$BIN_DIR/server > $OUTPUT_DIR/server.log 2>&1 &

# Store the server process ID to kill it later if needed
SERVER_PID=$!

# Run the specified number of clients
for (( i=1; i<=$1; i++ ))
do
  $BIN_DIR/client --name=$i > "$OUTPUT_DIR/client$i.log" 2>&1 &
done

# Wait for all clients to finish
wait

# Optionally, you can stop the server if it's no longer needed
cleanup