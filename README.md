# Chronos: A Distributed SQL Database in Rust

Chronos is a distributed SQL database built from scratch in Rust. It implements the Raft consensus algorithm for fault tolerance and consistency across multiple nodes.

## Features

- SQL parser for `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, and `DELETE`.
- Basic transaction support with `BEGIN`, `COMMIT`, and `ROLLBACK`.
- Simple, single-node storage engine using CSV files.
- Raft consensus algorithm for distributed log replication and leader election.
- gRPC-based networking for communication between nodes.
- Interactive REPL for both local and distributed operation.
- Dynamic index creation and persistence on `INSERT` operations.

## Architecture

Chronos is built with a modular architecture:

1. **Parser**: Converts SQL strings into an Abstract Syntax Tree (AST)
2. **Executor**: Executes the AST by interacting with the storage engine
3. **Storage**: Manages data persistence using CSV files
4. **Raft**: Implements the Raft consensus algorithm for distributed operation
5. **Network**: Provides gRPC-based communication between nodes
6. **REPL**: Provides an interactive command-line interface

## Usage

### Starting a Server Node

Each node in the cluster runs as a separate server process. To start the first node:

```bash
# Start the first node of the cluster
cargo run -- node --id node1 --address 127.0.0.1:8001 --data-dir data
```

To add more nodes, point them to the existing peers:

```bash
# Start a second node and connect it to the first
cargo run -- node --id node2 --address 127.0.0.1:8002 --data-dir data --peers node1=127.0.0.1:8001
```

### Using the Client REPL

Chronos provides an interactive REPL to execute SQL commands.

**Local Mode (Single-Node):**

To run a simple, local instance of Chronos without a separate server process, start the client directly. It will use a local CSV storage backend.

```bash
cargo run -- client
```

**Distributed Mode:**

To connect to a running cluster, specify the address of the leader node.

```bash
cargo run -- client --leader 127.0.0.1:8001
```

## SQL Examples

```sql
-- Create a table with a primary key
CREATE TABLE users (id INT PRIMARY KEY, name STRING, age INT);

-- Insert some data
INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30);
INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25);

-- Select all users
SELECT id, name, age FROM users;

-- Update a record
UPDATE users SET age = 31 WHERE name = 'Alice';

-- Delete a record
DELETE FROM users WHERE name = 'Bob';

-- Use a transaction
BEGIN;
INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 40);
COMMIT;
```

## Development Status

Chronos is a learning project and is not intended for production use. It currently implements:

- Basic SQL parsing and execution
- Simple CSV-based storage
- Raft consensus algorithm for leader election and log replication
- gRPC-based networking for node communication

Future work may include:

- More comprehensive SQL support (JOINs, aggregations)
- More robust error handling and recovery
- Performance optimizations

## License

This project is licensed under the MIT License - see the LICENSE.md file for details.
