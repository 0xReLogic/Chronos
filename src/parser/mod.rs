pub mod ast;
pub mod error;

use pest::Parser as PestParser;
use pest_derive::Parser;

pub use self::ast::{Ast, ColumnDefinition, DataType, Statement, Value, Condition};
pub use self::error::ParserError;

#[derive(Parser)]
#[grammar = "parser/sql.pest"]
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
            Ast::Statement(Statement::CreateTable { table_name, columns }) => {
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
            },
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_insert() {
        let sql = "INSERT INTO users VALUES ('John', 30);";
        let ast = Parser::parse(sql).unwrap();
        
        match ast {
            Ast::Statement(Statement::Insert { table_name, values, .. }) => {
                assert_eq!(table_name, "users");
                assert_eq!(values.len(), 2);
            },
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_select() {
        let sql = "SELECT * FROM users;";
        let ast = Parser::parse(sql).unwrap();
        
        match ast {
            Ast::Statement(Statement::Select { table_name, columns, .. }) => {
                assert_eq!(table_name, "users");
                assert_eq!(columns, vec!["*"]);
            },
            _ => panic!("Expected Select statement"),
        }
    }
}