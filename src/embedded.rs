use tokio::sync::Mutex;

use crate::executor::{Executor, ExecutorError, QueryResult};
use crate::parser::{Ast, Parser};

/// Embedded, in-process Chronos instance for edge applications.
///
/// This runs entirely in-process without starting a gRPC server or Raft
/// node. It uses the same SQL parser and Executor as the main node
/// binary, backed by the configured storage engine (currently Sled).
pub struct ChronosEmbedded {
    executor: Mutex<Executor>,
}

impl ChronosEmbedded {
    /// Create a new embedded Chronos instance using the given data directory.
    ///
    /// The data directory is passed through to the underlying Executor and
    /// storage engine. If the directory does not exist, it will be created
    /// by the storage layer.
    pub async fn new(data_dir: &str) -> Self {
        let executor = Executor::new(data_dir).await;
        Self {
            executor: Mutex::new(executor),
        }
    }

    /// Execute a SQL statement against the embedded database.
    ///
    /// This parses the SQL string into an AST and then forwards it to the
    /// underlying Executor. Parser errors are mapped into `ExecutorError`.
    pub async fn execute(&self, sql: &str) -> Result<QueryResult, ExecutorError> {
        let ast: Ast =
            Parser::parse(sql).map_err(|e| ExecutorError::ExecutionError(e.to_string()))?;

        let mut exec = self.executor.lock().await;
        exec.execute(ast).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn embedded_create_insert_select() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().to_str().unwrap();

        let db = ChronosEmbedded::new(path).await;

        db.execute("CREATE TABLE sensors (id INT, temperature FLOAT);")
            .await
            .expect("create table");

        db.execute("INSERT INTO sensors (id, temperature) VALUES (1, 25.5);")
            .await
            .expect("insert row");

        let result = db
            .execute("SELECT id, temperature FROM sensors;")
            .await
            .expect("select rows");

        assert_eq!(result.columns, vec!["id", "temperature"]);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].len(), 2);
    }
}
