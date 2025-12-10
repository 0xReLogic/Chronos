pub mod ast;
pub mod error;

use pest::Parser as PestParser;
use pest_derive::Parser;

pub use self::ast::{
    Assignment, Ast, ColumnDefinition, Condition, DataType, Operator, Statement, TtlSpec, Value,
};
pub use self::error::ParserError;

#[derive(Parser)]
#[grammar = "parser/chronos.pest"]
struct SqlParser;

pub struct Parser;

impl Parser {
    pub fn parse(input: &str) -> Result<Ast, ParserError> {
        let pairs = SqlParser::parse(Rule::sql, input)
            .map_err(|e| ParserError::PestError(e.to_string()))?;

        ast::parse_ast(pairs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table() {
        let sql = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::CreateTable {
                table_name,
                columns,
                ..
            }) => {
                assert_eq!(table_name, "users");
                assert_eq!(columns.len(), 3);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[0].data_type, DataType::Int);
                assert!(columns[0].primary_key);
                assert_eq!(columns[1].name, "name");
                assert_eq!(columns[1].data_type, DataType::Text);
                assert!(!columns[1].primary_key);
                assert_eq!(columns[2].name, "age");
                assert_eq!(columns[2].data_type, DataType::Int);
                assert!(!columns[2].primary_key);
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_insert() {
        let sql = "INSERT INTO users VALUES ('John', 30);";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::Insert {
                table_name, values, ..
            }) => {
                assert_eq!(table_name, "users");
                assert_eq!(values.len(), 2);
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_select() {
        let sql = "SELECT * FROM users;";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::Select {
                table_name,
                columns,
                ..
            }) => {
                assert_eq!(table_name, "users");
                assert_eq!(columns, vec!["*"]);
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_update() {
        let sql = "UPDATE users SET age = 31 WHERE name = 'John';";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::Update {
                table_name,
                assignments,
                conditions,
            }) => {
                assert_eq!(table_name, "users");
                assert_eq!(assignments.len(), 1);
                assert_eq!(assignments[0].column_name, "age");
                assert_eq!(assignments[0].value, Value::Integer(31));

                let conds = conditions.unwrap();
                assert_eq!(conds.len(), 1);
                assert_eq!(conds[0].column_name, "name");
                assert_eq!(conds[0].operator, Operator::Equals);
                assert_eq!(conds[0].value, Value::String("John".to_string()));
            }
            _ => panic!("Expected Update statement"),
        }
    }

    #[test]
    fn test_delete() {
        let sql = "DELETE FROM users WHERE age > 30;";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::Delete {
                table_name,
                conditions,
            }) => {
                assert_eq!(table_name, "users");

                let conds = conditions.unwrap();
                assert_eq!(conds.len(), 1);
                assert_eq!(conds[0].column_name, "age");
                assert_eq!(conds[0].operator, Operator::GreaterThan);
                assert_eq!(conds[0].value, Value::Integer(30));
            }
            _ => panic!("Expected Delete statement"),
        }
    }

    #[test]
    fn test_create_index() {
        let sql = "CREATE INDEX idx_users_on_name ON users(name);";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::CreateIndex {
                index_name,
                table_name,
                column_name,
            }) => {
                assert_eq!(index_name, "idx_users_on_name");
                assert_eq!(table_name, "users");
                assert_eq!(column_name, "name");
            }
            _ => panic!("Expected CreateIndex statement"),
        }
    }

    #[test]
    fn test_select_avg_1h() {
        let sql = "SELECT AVG_1H(temp) FROM sensors;";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::SelectAgg1h {
                table_name,
                column_name,
            }) => {
                assert_eq!(table_name, "sensors");
                assert_eq!(column_name, "temp");
            }
            _ => panic!("Expected SelectAgg1h statement"),
        }
    }

    #[test]
    fn test_select_avg_24h() {
        let sql = "SELECT AVG_24H(temp) FROM sensors;";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::SelectAgg24h {
                table_name,
                column_name,
            }) => {
                assert_eq!(table_name, "sensors");
                assert_eq!(column_name, "temp");
            }
            _ => panic!("Expected SelectAgg24h statement"),
        }
    }

    #[test]
    fn test_select_avg_7d() {
        let sql = "SELECT AVG_7D(temp) FROM sensors;";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::SelectAgg7d {
                table_name,
                column_name,
            }) => {
                assert_eq!(table_name, "sensors");
                assert_eq!(column_name, "temp");
            }
            _ => panic!("Expected SelectAgg7d statement"),
        }
    }

    #[test]
    fn test_select_count_star() {
        let sql = "SELECT COUNT(*) FROM sensors;";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::SelectCount {
                table_name,
                column,
                conditions,
            }) => {
                assert_eq!(table_name, "sensors");
                assert!(column.is_none());
                assert!(conditions.is_none());
            }
            _ => panic!("Expected SelectCount statement"),
        }
    }

    #[test]
    fn test_select_sum_with_where() {
        let sql = "SELECT SUM(temp) FROM sensors WHERE temp > 0;";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::SelectSum {
                table_name,
                column_name,
                conditions,
            }) => {
                assert_eq!(table_name, "sensors");
                assert_eq!(column_name, "temp");

                let conds = conditions.expect("expected conditions");
                assert_eq!(conds.len(), 1);
                assert_eq!(conds[0].column_name, "temp");
            }
            _ => panic!("Expected SelectSum statement"),
        }
    }

    #[test]
    fn test_select_join_using() {
        let sql = "SELECT * FROM sensors JOIN devices USING (device_id);";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::SelectJoin {
                left_table,
                right_table,
                join_column,
                columns,
                ..
            }) => {
                assert_eq!(left_table, "sensors");
                assert_eq!(right_table, "devices");
                assert_eq!(join_column, "device_id");
                assert_eq!(columns, vec!["*".to_string()]);
            }
            _ => panic!("Expected SelectJoin statement"),
        }
    }

    #[test]
    fn test_select_group_count_simple() {
        let sql = "SELECT device, COUNT(*) FROM sensors GROUP BY device;";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::SelectGroupCount {
                table_name,
                group_column,
                conditions,
            }) => {
                assert_eq!(table_name, "sensors");
                assert_eq!(group_column, "device");
                assert!(conditions.is_none());
            }
            _ => panic!("Expected SelectGroupCount statement"),
        }
    }

    #[test]
    fn test_select_group_count_with_where() {
        let sql = "SELECT device, COUNT(*) FROM sensors WHERE temperature > 0 GROUP BY device;";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::SelectGroupCount {
                table_name,
                group_column,
                conditions,
            }) => {
                assert_eq!(table_name, "sensors");
                assert_eq!(group_column, "device");

                let conds = conditions.expect("expected conditions");
                assert_eq!(conds.len(), 1);
                assert_eq!(conds[0].column_name, "temperature");
            }
            _ => panic!("Expected SelectGroupCount statement"),
        }
    }

    #[test]
    fn test_select_count_column_with_where() {
        let sql = "SELECT COUNT(temp) FROM sensors WHERE temp > 0;";
        let ast = Parser::parse(sql).unwrap();

        match ast {
            Ast::Statement(Statement::SelectCount {
                table_name,
                column,
                conditions,
            }) => {
                assert_eq!(table_name, "sensors");
                assert_eq!(column, Some("temp".to_string()));

                let conds = conditions.expect("expected conditions");
                assert_eq!(conds.len(), 1);
                assert_eq!(conds[0].column_name, "temp");
            }
            _ => panic!("Expected SelectCount statement"),
        }
    }
}
