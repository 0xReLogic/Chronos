use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use crate::parser::{Parser, Value};
use crate::executor::Executor;

pub struct Repl {
    executor: Option<Executor>,
    rl: DefaultEditor,
    distributed_mode: bool,
    sql_client: Option<crate::network::SqlClient>,
}

impl Repl {
    pub fn new(data_dir: &str) -> Self {
        Self {
            executor: Some(Executor::new(data_dir)),
            rl: DefaultEditor::new().expect("Failed to create line editor"),
            distributed_mode: false,
            sql_client: None,
        }
    }
    
    pub fn with_distributed_mode(leader_address: &str) -> Self {
        Self {
            executor: None,
            rl: DefaultEditor::new().expect("Failed to create line editor"),
            distributed_mode: true,
            sql_client: Some(crate::network::SqlClient::new(leader_address)),
        }
    }
    
    pub async fn run(&mut self) {
        println!("Welcome to Chronos SQL Database");
        if self.distributed_mode {
            println!("Running in distributed mode");
        } else {
            println!("Running in single-node mode");
        }
        println!("Enter SQL statements or 'exit' to quit");
        
        loop {
            let readline = self.rl.readline("chronos> ");
            match readline {
                Ok(line) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    
                    let _ = self.rl.add_history_entry(line.as_str());
                    
                    if line.trim().eq_ignore_ascii_case("exit") {
                        println!("Goodbye!");
                        break;
                    }
                    
                    if self.distributed_mode {
                        // In distributed mode, send the SQL to the leader
                        if let Some(client) = &mut self.sql_client {
                            // Since we are in an async method, we can .await directly
                            match client.execute_sql(&line).await {
                                Ok(response) => {
                                    if !response.success {
                                        eprintln!("Error: {}", response.error);
                                        continue;
                                    }
                                    
                                    // Print column headers
                                    let header_width = 20;
                                    for col in &response.columns {
                                        print!("{:width$}", col, width = header_width);
                                    }
                                    println!();
                                    
                                    // Print separator
                                    for _ in 0..response.columns.len() {
                                        print!("{}", "-".repeat(header_width));
                                    }
                                    println!();
                                    
                                    // Print rows
                                    for row in response.rows {
                                        for value in row.values {
                                            let display = match value.value {
                                                Some(crate::network::proto::value::Value::StringValue(s)) => s,
                                                Some(crate::network::proto::value::Value::IntValue(i)) => i.to_string(),
                                                Some(crate::network::proto::value::Value::FloatValue(f)) => f.to_string(),
                                                Some(crate::network::proto::value::Value::BoolValue(b)) => b.to_string(),
                                                Some(crate::network::proto::value::Value::NullValue(_)) => "NULL".to_string(),
                                                None => "NULL".to_string(),
                                            };
                                            print!("{:width$}", display, width = header_width);
                                        }
                                        println!();
                                    }
                                },
                                Err(e) => {
                                    eprintln!("Network error: {}", e);
                                },
                            }
                        } else {
                            eprintln!("SQL client not initialized");
                        }
                    } else {
                        // In single-node mode, execute locally
                        let executor = self.executor.as_mut().expect("Executor not initialized in single-node mode");
                        match Parser::parse(&line) {
                            Ok(ast) => {
                                match executor.execute(ast) {
                                    Ok(result) => {
                                        // Print column headers
                                        let header_width = 20;
                                        for col in &result.columns {
                                            print!("{:width$}", col, width = header_width);
                                        }
                                        println!();
                                        
                                        // Print separator
                                        for _ in 0..result.columns.len() {
                                            print!("{}", "-".repeat(header_width));
                                        }
                                        println!();
                                        
                                        // Print rows
                                        for row in result.rows {
                                            for value in row {
                                                let display = match value {
                                                    Value::String(s) => s,
                                                    Value::Integer(i) => i.to_string(),
                                                    Value::Float(f) => f.to_string(),
                                                    Value::Boolean(b) => b.to_string(),
                                                    Value::Null => "NULL".to_string(),
                                                };
                                                print!("{:width$}", display, width = header_width);
                                            }
                                            println!();
                                        }
                                    },
                                    Err(e) => {
                                        eprintln!("Error: {}", e);
                                    },
                                }
                            },
                            Err(e) => {
                                eprintln!("Parse error: {}", e);
                            },
                        }
                    }
                },
                Err(ReadlineError::Interrupted) => {
                    println!("CTRL-C");
                    break;
                },
                Err(ReadlineError::Eof) => {
                    println!("CTRL-D");
                    break;
                },
                Err(err) => {
                    eprintln!("Error: {}", err);
                    break;
                },
            }
        }
    }
}