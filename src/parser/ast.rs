use super::{ParserError, Rule};
use pest::iterators::Pairs;

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub enum DataType {
    Int,
    Text,
    Float,
    Boolean,
    String,
}

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub primary_key: bool,
}

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub struct TtlSpec {
    pub seconds: u64,
}

fn parse_ttl_value(raw: &str) -> Result<TtlSpec, ParserError> {
    // raw format: digits + optional unit (s|m|h|d)
    if raw.is_empty() {
        return Err(ParserError::InvalidSyntax("TTL value missing".to_string()));
    }
    let (num_part, unit_part) =
        raw.split_at(raw.chars().take_while(|c| c.is_ascii_digit()).count());
    if num_part.is_empty() {
        return Err(ParserError::InvalidSyntax(
            "TTL numeric part missing".to_string(),
        ));
    }
    let n: u64 = num_part
        .parse()
        .map_err(|_| ParserError::InvalidSyntax("TTL parse error".to_string()))?;
    let seconds = match unit_part {
        "s" | "" => n,
        "m" => n * 60,
        "h" => n * 60 * 60,
        "d" => n * 60 * 60 * 24,
        _ => return Err(ParserError::InvalidSyntax("TTL unit invalid".to_string())),
    };
    Ok(TtlSpec { seconds })
}

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s) => write!(f, "{s}"),
            Value::Integer(i) => write!(f, "{i}"),
            Value::Float(fl) => write!(f, "{fl}"),
            Value::Boolean(b) => write!(f, "{b}"),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            _ => None, // Incomparable types
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub enum Operator {
    Equals,
    NotEquals,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
}

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub struct Condition {
    pub column_name: String,
    pub operator: Operator,
    pub value: Value,
}

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub struct Assignment {
    pub column_name: String,
    pub value: Value,
}

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub enum Statement {
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDefinition>,
        ttl: Option<TtlSpec>,
    },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Value>,
    },
    Select {
        table_name: String,
        columns: Vec<String>,
        conditions: Option<Vec<Condition>>,
    },
    SelectAgg1h {
        table_name: String,
        column_name: String,
    },
    SelectAgg24h {
        table_name: String,
        column_name: String,
    },
    SelectAgg7d {
        table_name: String,
        column_name: String,
    },
    SelectCount {
        table_name: String,
        column: Option<String>,
        conditions: Option<Vec<Condition>>, // COUNT(* or column) with optional WHERE
    },
    SelectSum {
        table_name: String,
        column_name: String,
        conditions: Option<Vec<Condition>>, // SUM(column) with optional WHERE
    },
    SelectAvg {
        table_name: String,
        column_name: String,
        conditions: Option<Vec<Condition>>, // AVG(column) with optional WHERE
    },
    SelectMin {
        table_name: String,
        column_name: String,
        conditions: Option<Vec<Condition>>, // MIN(column) with optional WHERE
    },
    SelectMax {
        table_name: String,
        column_name: String,
        conditions: Option<Vec<Condition>>, // MAX(column) with optional WHERE
    },
    SelectJoin {
        left_table: String,
        right_table: String,
        join_column: String,
        columns: Vec<String>,
        conditions: Option<Vec<Condition>>, // SELECT ... FROM left JOIN right USING (col) [WHERE ...]
    },
    SelectGroupCount {
        table_name: String,
        group_column: String,
        conditions: Option<Vec<Condition>>, // SELECT col, COUNT(*) ... GROUP BY col with optional WHERE
    },
    Begin,
    Commit,
    Rollback,
    Update {
        table_name: String,
        assignments: Vec<Assignment>,
        conditions: Option<Vec<Condition>>,
    },
    Delete {
        table_name: String,
        conditions: Option<Vec<Condition>>,
    },
    CreateIndex {
        index_name: String,
        table_name: String,
        column_name: String,
    },
}

#[derive(
    Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode,
)]
pub enum Ast {
    Statement(Statement),
}

pub fn parse_ast(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    for pair in pairs {
        match pair.as_rule() {
            Rule::sql => {
                // Recursively process the inner pairs
                return parse_ast(pair.into_inner());
            }
            Rule::sql_stmt => {
                // Process the SQL statement
                return parse_statement(pair.into_inner());
            }
            _ => {}
        }
    }

    Err(ParserError::InvalidSyntax("Invalid SQL syntax".to_string()))
}

fn parse_statement(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    for pair in pairs {
        match pair.as_rule() {
            Rule::create_table_stmt => {
                return parse_create_table(pair.into_inner());
            }
            Rule::create_index_stmt => {
                return parse_create_index(pair.into_inner());
            }
            Rule::insert_stmt => {
                return parse_insert(pair.into_inner());
            }
            Rule::select_agg_1h_stmt => {
                return parse_select_agg_1h(pair.into_inner());
            }
            Rule::select_agg_24h_stmt => {
                return parse_select_agg_24h(pair.into_inner());
            }
            Rule::select_agg_7d_stmt => {
                return parse_select_agg_7d(pair.into_inner());
            }
            Rule::select_count_stmt => {
                return parse_select_count(pair.into_inner());
            }
            Rule::select_sum_stmt => {
                return parse_select_sum(pair.into_inner());
            }
            Rule::select_avg_stmt => {
                return parse_select_avg(pair.into_inner());
            }
            Rule::select_min_stmt => {
                return parse_select_min(pair.into_inner());
            }
            Rule::select_max_stmt => {
                return parse_select_max(pair.into_inner());
            }
            Rule::select_group_count_stmt => {
                return parse_select_group_count(pair.into_inner());
            }
            Rule::select_join_stmt => {
                return parse_select_join(pair.into_inner());
            }
            Rule::select_stmt => {
                return parse_select(pair.into_inner());
            }
            Rule::begin_stmt => {
                return Ok(Ast::Statement(Statement::Begin));
            }
            Rule::commit_stmt => {
                return Ok(Ast::Statement(Statement::Commit));
            }
            Rule::rollback_stmt => {
                return Ok(Ast::Statement(Statement::Rollback));
            }
            Rule::update_stmt => {
                return parse_update(pair.into_inner());
            }
            Rule::delete_stmt => {
                return parse_delete(pair.into_inner());
            }
            _ => {}
        }
    }

    Err(ParserError::InvalidSyntax("Invalid statement".to_string()))
}

fn parse_create_table(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut table_name = String::new();
    let mut columns = Vec::new();
    let mut ttl: Option<TtlSpec> = None;

    for pair in pairs {
        match pair.as_rule() {
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            Rule::column_list => {
                for column_pair in pair.into_inner() {
                    if column_pair.as_rule() == Rule::column_definition {
                        let column = parse_column_definition(column_pair.into_inner())?;
                        columns.push(column);
                    }
                }
            }
            Rule::ttl_clause => {
                for inner in pair.into_inner() {
                    if inner.as_rule() == Rule::ttl_value {
                        ttl = Some(parse_ttl_value(inner.as_str())?);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::CreateTable {
        table_name,
        columns,
        ttl,
    }))
}

fn parse_column_definition(pairs: Pairs<Rule>) -> Result<ColumnDefinition, ParserError> {
    let mut name = String::new();
    let mut data_type = DataType::Text; // Default
    let mut primary_key = false;

    for pair in pairs {
        match pair.as_rule() {
            Rule::column_name => {
                name = pair.as_str().to_string();
            }
            Rule::data_type => {
                data_type = match pair.as_str().to_uppercase().as_str() {
                    "INT" => DataType::Int,
                    "TEXT" => DataType::Text,
                    "FLOAT" => DataType::Float,
                    "BOOLEAN" => DataType::Boolean,
                    "STRING" => DataType::String,
                    _ => return Err(ParserError::InvalidDataType(pair.as_str().to_string())),
                };
            }
            Rule::PRIMARY => {
                // Next token should be KEY
                primary_key = true;
            }
            _ => {}
        }
    }

    Ok(ColumnDefinition {
        name,
        data_type,
        primary_key,
    })
}

fn parse_insert(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut table_name = String::new();
    let mut columns: Option<Vec<String>> = None;
    let mut values = Vec::new();

    for pair in pairs {
        match pair.as_rule() {
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            Rule::column_name => {
                let column_name = pair.as_str().to_string();
                if let Some(ref mut cols) = columns {
                    cols.push(column_name);
                } else {
                    columns = Some(vec![column_name]);
                }
            }
            Rule::value_list => {
                for value_pair in pair.into_inner() {
                    if value_pair.as_rule() == Rule::value {
                        let value = parse_value(value_pair.into_inner())?;
                        values.push(value);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::Insert {
        table_name,
        columns,
        values,
    }))
}

fn parse_value(pairs: Pairs<Rule>) -> Result<Value, ParserError> {
    for pair in pairs {
        if pair.as_rule() == Rule::literal {
            return parse_literal(pair.into_inner());
        }
    }

    Err(ParserError::InvalidValue("Invalid value".to_string()))
}

fn parse_literal(pairs: Pairs<Rule>) -> Result<Value, ParserError> {
    for pair in pairs {
        match pair.as_rule() {
            Rule::string_literal => {
                // Remove the surrounding quotes
                let s = pair.as_str();
                let s = &s[1..s.len() - 1];
                return Ok(Value::String(s.to_string()));
            }
            Rule::integer_literal => {
                let i = pair.as_str().parse::<i64>().map_err(|_| {
                    ParserError::InvalidValue(format!("Invalid integer: {}", pair.as_str()))
                })?;
                return Ok(Value::Integer(i));
            }
            Rule::float_literal => {
                let f = pair.as_str().parse::<f64>().map_err(|_| {
                    ParserError::InvalidValue(format!("Invalid float: {}", pair.as_str()))
                })?;
                return Ok(Value::Float(f));
            }
            Rule::boolean_literal => {
                let b = pair.as_str().to_uppercase() == "TRUE";
                return Ok(Value::Boolean(b));
            }
            Rule::null_literal => {
                return Ok(Value::Null);
            }
            _ => {}
        }
    }

    Err(ParserError::InvalidValue("Invalid literal".to_string()))
}

fn parse_select(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut table_name = String::new();
    let mut columns = Vec::new();
    let mut conditions = None;

    for pair in pairs {
        match pair.as_rule() {
            Rule::column_selector => {
                if pair.as_str() == "*" {
                    columns.push("*".to_string());
                } else {
                    for column_pair in pair.into_inner() {
                        if column_pair.as_rule() == Rule::column_name {
                            columns.push(column_pair.as_str().to_string());
                        }
                    }
                }
            }
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            Rule::where_clause => {
                conditions = Some(parse_where_clause(pair.into_inner())?);
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::Select {
        table_name,
        columns,
        conditions,
    }))
}

fn parse_select_agg_1h(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut table_name = String::new();
    let mut column_name = String::new();

    for pair in pairs {
        match pair.as_rule() {
            Rule::column_name => {
                column_name = pair.as_str().to_string();
            }
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::SelectAgg1h {
        table_name,
        column_name,
    }))
}

fn parse_select_agg_24h(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut table_name = String::new();
    let mut column_name = String::new();

    for pair in pairs {
        match pair.as_rule() {
            Rule::column_name => {
                column_name = pair.as_str().to_string();
            }
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::SelectAgg24h {
        table_name,
        column_name,
    }))
}

fn parse_select_agg_7d(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut table_name = String::new();
    let mut column_name = String::new();

    for pair in pairs {
        match pair.as_rule() {
            Rule::column_name => {
                column_name = pair.as_str().to_string();
            }
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::SelectAgg7d {
        table_name,
        column_name,
    }))
}

fn parse_select_count(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut table_name = String::new();
    let mut column: Option<String> = None;
    let mut conditions = None;

    for pair in pairs {
        match pair.as_rule() {
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            Rule::column_name => {
                // COUNT(column_name)
                column = Some(pair.as_str().to_string());
            }
            Rule::where_clause => {
                conditions = Some(parse_where_clause(pair.into_inner())?);
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::SelectCount {
        table_name,
        column,
        conditions,
    }))
}

fn parse_select_group_count(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut table_name = String::new();
    let mut group_column = String::new();
    let mut group_by_column: Option<String> = None;
    let mut conditions = None;

    for pair in pairs {
        match pair.as_rule() {
            Rule::column_name => {
                if group_column.is_empty() {
                    group_column = pair.as_str().to_string();
                }
            }
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            Rule::where_clause => {
                conditions = Some(parse_where_clause(pair.into_inner())?);
            }
            Rule::group_by_clause => {
                for inner in pair.into_inner() {
                    if inner.as_rule() == Rule::column_name {
                        group_by_column = Some(inner.as_str().to_string());
                    }
                }
            }
            _ => {}
        }
    }

    if let Some(gb_col) = group_by_column {
        if gb_col != group_column {
            return Err(ParserError::InvalidSyntax(
                "GROUP BY column must match selected group column".to_string(),
            ));
        }
    }

    Ok(Ast::Statement(Statement::SelectGroupCount {
        table_name,
        group_column,
        conditions,
    }))
}

fn parse_select_sum(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut table_name = String::new();
    let mut column_name = String::new();
    let mut conditions = None;

    for pair in pairs {
        match pair.as_rule() {
            Rule::column_name => {
                column_name = pair.as_str().to_string();
            }
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            Rule::where_clause => {
                conditions = Some(parse_where_clause(pair.into_inner())?);
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::SelectSum {
        table_name,
        column_name,
        conditions,
    }))
}

fn parse_select_avg(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut table_name = String::new();
    let mut column_name = String::new();
    let mut conditions = None;

    for pair in pairs {
        match pair.as_rule() {
            Rule::column_name => {
                column_name = pair.as_str().to_string();
            }
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            Rule::where_clause => {
                conditions = Some(parse_where_clause(pair.into_inner())?);
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::SelectAvg {
        table_name,
        column_name,
        conditions,
    }))
}

fn parse_select_min(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut table_name = String::new();
    let mut column_name = String::new();
    let mut conditions = None;

    for pair in pairs {
        match pair.as_rule() {
            Rule::column_name => {
                column_name = pair.as_str().to_string();
            }
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            Rule::where_clause => {
                conditions = Some(parse_where_clause(pair.into_inner())?);
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::SelectMin {
        table_name,
        column_name,
        conditions,
    }))
}

fn parse_select_max(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut table_name = String::new();
    let mut column_name = String::new();
    let mut conditions = None;

    for pair in pairs {
        match pair.as_rule() {
            Rule::column_name => {
                column_name = pair.as_str().to_string();
            }
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            Rule::where_clause => {
                conditions = Some(parse_where_clause(pair.into_inner())?);
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::SelectMax {
        table_name,
        column_name,
        conditions,
    }))
}

fn parse_select_join(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut left_table = String::new();
    let mut right_table = String::new();
    let mut join_column = String::new();
    let mut columns = Vec::new();
    let mut conditions = None;

    for pair in pairs {
        match pair.as_rule() {
            Rule::column_selector => {
                if pair.as_str() == "*" {
                    columns.push("*".to_string());
                } else {
                    for column_pair in pair.into_inner() {
                        if column_pair.as_rule() == Rule::column_name {
                            columns.push(column_pair.as_str().to_string());
                        }
                    }
                }
            }
            Rule::table_name => {
                if left_table.is_empty() {
                    left_table = pair.as_str().to_string();
                } else {
                    right_table = pair.as_str().to_string();
                }
            }
            Rule::column_name => {
                // JOIN ... USING(column_name)
                if join_column.is_empty() {
                    join_column = pair.as_str().to_string();
                }
            }
            Rule::where_clause => {
                conditions = Some(parse_where_clause(pair.into_inner())?);
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::SelectJoin {
        left_table,
        right_table,
        join_column,
        columns,
        conditions,
    }))
}

fn parse_create_index(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut index_name = String::new();
    let mut table_name = String::new();
    let mut column_name = String::new();

    for pair in pairs {
        match pair.as_rule() {
            Rule::identifier => {
                index_name = pair.as_str().to_string();
            }
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            Rule::column_name => {
                column_name = pair.as_str().to_string();
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::CreateIndex {
        index_name,
        table_name,
        column_name,
    }))
}

fn parse_where_clause(pairs: Pairs<Rule>) -> Result<Vec<Condition>, ParserError> {
    let mut conditions = Vec::new();

    for pair in pairs {
        if pair.as_rule() == Rule::condition {
            let condition = parse_condition(pair.into_inner())?;
            conditions.push(condition);
        }
    }

    Ok(conditions)
}

fn parse_condition(pairs: Pairs<Rule>) -> Result<Condition, ParserError> {
    let mut column_name = String::new();
    let mut operator_str = String::new();
    let mut value = Value::Null;

    for pair in pairs {
        match pair.as_rule() {
            Rule::column_name => {
                column_name = pair.as_str().to_string();
            }
            Rule::comparison_operator => {
                operator_str = pair.as_str().to_string();
            }
            Rule::value => {
                value = parse_value(pair.into_inner())?;
            }
            _ => {}
        }
    }

    let operator = match operator_str.as_str() {
        "=" => Operator::Equals,
        "!=" => Operator::NotEquals,
        "<" => Operator::LessThan,
        ">" => Operator::GreaterThan,
        "<=" => Operator::LessThanOrEqual,
        ">=" => Operator::GreaterThanOrEqual,
        _ => return Err(ParserError::InvalidOperator(operator_str)),
    };

    Ok(Condition {
        column_name,
        operator,
        value,
    })
}

fn parse_update(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut table_name = String::new();
    let mut assignments = Vec::new();
    let mut conditions = None;

    for pair in pairs {
        match pair.as_rule() {
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            Rule::assignment_list => {
                assignments = parse_assignment_list(pair.into_inner())?;
            }
            Rule::where_clause => {
                conditions = Some(parse_where_clause(pair.into_inner())?);
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::Update {
        table_name,
        assignments,
        conditions,
    }))
}

fn parse_delete(pairs: Pairs<Rule>) -> Result<Ast, ParserError> {
    let mut table_name = String::new();
    let mut conditions = None;

    for pair in pairs {
        match pair.as_rule() {
            Rule::table_name => {
                table_name = pair.as_str().to_string();
            }
            Rule::where_clause => {
                conditions = Some(parse_where_clause(pair.into_inner())?);
            }
            _ => {}
        }
    }

    Ok(Ast::Statement(Statement::Delete {
        table_name,
        conditions,
    }))
}

fn parse_assignment_list(pairs: Pairs<Rule>) -> Result<Vec<Assignment>, ParserError> {
    let mut assignments = Vec::new();

    for pair in pairs {
        if let Rule::assignment = pair.as_rule() {
            assignments.push(parse_assignment(pair.into_inner())?);
        }
    }

    Ok(assignments)
}

fn parse_assignment(pairs: Pairs<Rule>) -> Result<Assignment, ParserError> {
    let mut column_name = String::new();
    let mut value = Value::Null;

    for pair in pairs {
        match pair.as_rule() {
            Rule::column_name => {
                column_name = pair.as_str().to_string();
            }
            Rule::value => {
                value = parse_value(pair.into_inner())?;
            }
            _ => {}
        }
    }

    Ok(Assignment { column_name, value })
}
