use thiserror::Error;

#[derive(Error, Debug)]
pub enum ParserError {
    #[error("Pest parser error: {0}")]
    PestError(String),
    
    #[error("Invalid syntax: {0}")]
    InvalidSyntax(String),
    
    #[error("Invalid data type: {0}")]
    InvalidDataType(String),
    
    #[error("Invalid value: {0}")]
    InvalidValue(String),
    
    #[error("Invalid operator: {0}")]
    InvalidOperator(String),
}