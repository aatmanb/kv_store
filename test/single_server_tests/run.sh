#!/bin/bash

kill_clients() {
    echo "Stopping clients ..."
    for pid in "${client_pids[@]}"; do
        kill "$pid" 2>/dev/null
        wait "$pid" 2>/dev/null
    done
    echo "Clients stopped."
}

kill_server() { 
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Stopping the server..."
        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
        echo "Server stopped."
    fi
}

# Function to clean up processes on exit
cleanup() {
    kill_server
    kill_clients
    exit
}

# Trap SIGINT (Ctrl+C) and SIGTERM (kill) signals
trap cleanup EXIT SIGINT SIGTERM

# Check if the number of clients and output directory are provided as arguments
if [ $# -lt 2 ]; then
  echo "Usage: $0 <number_of_clients> <output_directory>"
  exit 1
fi

BASE_DIR=$(pwd)
ROOT_DIR=$BASE_DIR/../..
SRC_DIR=$ROOT_DIR/src
TEST_DIR=$BASE_DIR
BIN_DIR="$ROOT_DIR/bin"
DB_DIR="$BASE_DIR/db"

NUM_CLIENTS=$1
OUTPUT_DIR=$2
TEST_TYPE=$3

if [[ "$4" == "--gen-test-data" ]]; then
    echo "Generating test data"
    cd $TEST_DIR/..
    DATA_DIR="$(pwd)/data"
    if [ ! -d "$DATA_DIR" ]; then
    	echo "Creating data directory $DATA_DIR"
    	mkdir -p "$DATA_DIR"
    fi
    python3 generator.py --num-clients=$NUM_CLIENTS --base_dir=$TEST_DIR
    cd $ROOT_DIR
    echo "Generation complete"
fi


# Check if the output directory exists, if not create it
if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Creating log directory $OUTPUT_DIR"
    mkdir -p "$OUTPUT_DIR"
else
    echo "Clearing log directory $OUTPUT_DIR"
    rm -rf $OUTPUT_DIR/*
fi

if [ ! -d "$DB_DIR" ]; then
    echo "Creating database directory $DB_DIR"
    mkdir -p "$DB_DIR"
else
    echo "Clearing database directory $DB_DIR"
    rm -rf $DB_DIR/*
fi

echo "Starting server"
# Start the server in the background and redirect output to server.log
$BIN_DIR/server --db_dir="$DB_DIR" >| $OUTPUT_DIR/server.log 2>&1 &

# Store the server process ID to kill it later if needed
SERVER_PID=$!
echo "Server PID $SERVER_PID"

# wait for 1 sec before starting clients
sleep 1

client_pids=()

echo "Starting clients"
# Run the specified number of clients
for (( i=0; i<$NUM_CLIENTS; i++ ))
do
    args="--id=$i --real=$TEST_DIR/data/real$i.csv --fake=$TEST_DIR/data/fake$i.csv"
    if [[ "$TEST_TYPE" == "--crash-consistency-test" ]]; then
        args="$args --test_type=2"
    elif [[ "$TEST_TYPE" == "--performance-test" ]]; then
        args="$args --test_type=3"
    else
        args="$args --test_type=1"
    fi
    echo "Starting client $i"
    echo "$args"
    echo $TEST_DIR
    cd $TEST_DIR
    echo $OUTPUT_DIR
    python3 test.py $args >| $OUTPUT_DIR/client$i.log 2>&1 &
    client_pids+=($!)
done

echo "All clients have started"

if [[ "$TEST_TYPE" == "--crash-consistency-test" ]]; then
    for (( i=0; i<2; i++ ))
    do
        sleep 3
        kill_server
        sleep 5
        echo "Starting server"
        # Start the server in the background and redirect output to server.log
        $BIN_DIR/server --db_dir="$DB_DIR" >| $OUTPUT_DIR/server$i.log 2>&1 &
        # Store the server process ID to kill it later if needed
        SERVER_PID=$!
        echo "Server PID $SERVER_PID"
    done
fi

# Wait for all clients to finish
echo "Waiting for all the clients to finish"
for pid in "${client_pids[@]}"; do
    wait $pid  # Wait for each specific PID in the array
done
echo "All clients have finished"

# Optionally, you can stop the server if it's no longer needed
kill_server

echo "Exiting script."
exit
