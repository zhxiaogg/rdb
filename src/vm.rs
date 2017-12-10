use table::{Row, Table};
use sql;
use sql::ParsedSQL;

pub enum StatementType {
    SELECT,
    INSERT,
}

pub struct Statement {
    kind: StatementType,
    parsed: Option<ParsedSQL>,
    row_to_insert: Option<Row>,
}

pub trait VM {
    fn execute(&mut self, table: &mut Table) -> Result<(), String>;
}

impl Statement {
    fn new_select_statement(parsed: Option<ParsedSQL>) -> Statement {
        Statement {
            kind: StatementType::SELECT,
            row_to_insert: None,
            parsed: parsed,
        }
    }

    pub fn prepare(input_buffer: &str) -> Result<Statement, String> {
        if input_buffer.eq("select") {
            Result::Ok(Statement::new_select_statement(None))
        } else if input_buffer.starts_with("select") {
            sql::parse(input_buffer.as_bytes())
                .map(|parsed_sql| Statement::new_select_statement(Some(parsed_sql)))
        } else if input_buffer.starts_with("insert") {
            let parts: Vec<&str> = input_buffer.splitn(4, ' ').collect();
            if parts.len() != 4 {
                Result::Err(input_buffer.to_owned())
            } else {
                let id = i32::from_str_radix(parts[1], 10).unwrap();
                if id < 0 {
                    return Result::Err("ID must be positive.".to_owned());
                }
                let username = String::from(parts[2]);
                let email = String::from(parts[3]);
                if username.len() > 32 || email.len() > 256 {
                    return Result::Err("String is too long.".to_owned());
                }
                let statement = Statement {
                    kind: StatementType::INSERT,
                    row_to_insert: Some(Row {
                        id: id as u32,
                        username: username,
                        email: email,
                    }),
                    parsed: None,
                };
                Result::Ok(statement)
            }
        } else {
            Result::Err(format!("Unrecognized command: {}", input_buffer).to_owned())
        }
    }
}

impl VM for Statement {
    fn execute(&mut self, table: &mut Table) -> Result<(), String> {
        match self.kind {
            StatementType::SELECT if self.parsed.is_none() => {
                let mut cursor = table.select_cursor();
                while !cursor.end_of_table() {
                    let row = cursor.get();
                    println!("({}, {}, {})", row.id, &row.username, &row.email);
                    cursor.advance();
                }
                Result::Ok(())
            }
            StatementType::SELECT => Result::Ok(()),
            StatementType::INSERT => {
                if let Some(r) = self.row_to_insert.as_ref() {
                    table.insert_cursor(r.id).save(r)
                } else {
                    Result::Ok(())
                }
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_can_prepare_for_select_1() {
        let prepare_result = Statement::prepare("select 1");
        assert!(prepare_result.is_ok());
    }
}
