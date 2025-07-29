# Chronos: A Distributed SQL Database in Rust

Chronos is a distributed SQL database built from scratch in Rust. It implements the Raft consensus algorithm for fault tolerance and consistency across multiple nodes.

## Features

- SQL parser for basic SQL commands (CREATE TABLE, INSERT, SELECT)
- Simple storage engine using CSV files
- Raft consensus algorithm for distributed operation
- gRPC-based networking for communication between nodes
- Interactive REPL for both single-node and distributed operation

## Architecture

Chronos is built with a modular architecture:

1. **Parser**: Converts SQL strings into an Abstract Syntax Tree (AST)
2. **Executor**: Executes the AST by interacting with the storage engine
3. **Storage**: Manages data persistence using CSV files
4. **Raft**: Implements the Raft consensus algorithm for distributed operation
5. **Network**: Provides gRPC-based communication between nodes
6. **REPL**: Provides an interactive command-line interface

## Usage

### Single-Node Mode

```bash
cargo run -- single-node --data-dir data
```

### Distributed Mode (Multiple Nodes)

Start the first node (each node will create its own subdirectory, e.g., `data/node1`):
```bash
cargo run -- node --id node1 --address 127.0.0.1:8001 --data-dir data
```

To start a node from a clean state (useful for testing), use the `--clean` flag:
```bash
cargo run -- node --id node1 --address 127.0.0.1:8001 --data-dir data --clean
```

Start additional nodes:
```bash
cargo run -- node --id node2 --address 127.0.0.1:8002 --data-dir data --peers node1=127.0.0.1:8001
cargo run -- node --id node3 --address 127.0.0.1:8003 --data-dir data --peers node1=127.0.0.1:8001,node2=127.0.0.1:8002
```

### Client Mode

Connect to a running cluster:
```bash
cargo run -- client --leader 127.0.0.1:8001
```

## SQL Examples

```sql
-- Create a table
CREATE TABLE users (id INT, name STRING);

-- Insert data
INSERT INTO users (id, name) VALUES (1, 'Alice');
INSERT INTO users (id, name) VALUES (2, 'Bob');

-- Query data
SELECT id, name FROM users;
```

## Development Status

Chronos is a learning project and is not intended for production use. It currently implements:

- Basic SQL parsing and execution
- Simple CSV-based storage
- Raft consensus algorithm for leader election and log replication
- gRPC-based networking for node communication

Future work may include:

- More comprehensive SQL support
- Improved storage engine with indexing
- Transaction support
- Better error handling and recovery
- Performance optimizations

## License

This project is licensed under the MIT License - see the LICENSE.md file for details.
