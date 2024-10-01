#!/bin/bash

# Function to clean up processes on exit
cleanup() {
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Stopping the server..."
        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
        echo "Server stopped."
        exit
    fi
}

# Trap SIGINT (Ctrl+C) and SIGTERM (kill) signals
trap cleanup EXIT SIGINT SIGTERM

# Check if the number of clients and output directory are provided as arguments
if [ $# -ne 2 ]; then
  echo "Usage: $0 <number_of_clients> <output_directory>"
  exit 1
fi

ROOT_DIR=$(pwd)
SRC_DIR=$ROOT_DIR/src
TEST_DIR=$ROOT_DIR/test
BIN_DIR="$SRC_DIR/cmake/build"
DB_DIR="$ROOT_DIR/db"

NUM_CLIENTS=$1
OUTPUT_DIR=$2

# Check if the output directory exists, if not create it
if [ ! -d "$OUTPUT_DIR" ]; then
  mkdir -p "$OUTPUT_DIR"
fi

if [ ! -d "$DB_DIR" ]; then
  mkdir -p "$DB_DIR"
fi

rm -rf $DB_DIR/*

# Start the server in the background and redirect output to server.log
$BIN_DIR/server --db_dir="$DB_DIR" >| $OUTPUT_DIR/server.log & #2>&1 &

# Store the server process ID to kill it later if needed
SERVER_PID=$!

# wait for 1 sec before starting clients
sleep 1

# Run the specified number of clients
for (( i=1; i<=$1; i++ ))
do
  $BIN_DIR/test --id=$i --real=$TEST_DIR/data/real.csv --fake=$TEST_DIR/data/fake.csv >| "$OUTPUT_DIR/client$i.log" & # 2>&1 &
done

# Wait for all clients to finish
wait

# Optionally, you can stop the server if it's no longer needed
cleanup

echo "All clients have finished. Exiting script."
