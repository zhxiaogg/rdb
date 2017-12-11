use table::{Row, Table};
use table::schema::Schema;
use sql;
use sql::ParsedSQL;
use sql::SQLType;

mod row_buf;
use self::row_buf::RowBuf;
mod codegen;
use self::codegen::OpCode;

pub enum StatementType {
    SELECT,
    INSERT,
}

pub struct Statement {
    kind: StatementType,
    parsed: Option<ParsedSQL>,
    codes: Vec<OpCode>,
    row_to_insert: Option<Row>,
    // TODO: stack only support i64 now.
    stack: Vec<i64>,
    // TODO: use a bidirectional map sort thing.
    sym_table: Vec<String>,
    pub row_buf: RowBuf,
    pc: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ExecResult {
    PendingRow,
    Complete,
    Error(String),
}

pub trait VM {
    fn execute(&mut self, table: &mut Table) -> Result<(), String>;
    fn execute_codes(&mut self) -> ExecResult;
}

impl Statement {
    fn new_select_statement() -> Statement {
        Statement {
            kind: StatementType::SELECT,
            row_to_insert: None,
            parsed: None,
            codes: Vec::new(),
            stack: Vec::new(),
            sym_table: Vec::new(),
            row_buf: RowBuf::new(),
            pc: 0,
        }
    }

    fn new_select_statement2(parsed_sql: ParsedSQL, codes: Vec<OpCode>) -> Statement {
        Statement {
            kind: StatementType::SELECT,
            row_to_insert: None,
            parsed: Some(parsed_sql),
            codes: codes,
            stack: Vec::new(),
            sym_table: Vec::new(),
            row_buf: RowBuf::new(),
            pc: 0,
        }
    }

    pub fn prepare(input_buffer: &str, schema: &Schema) -> Result<Statement, String> {
        if input_buffer.eq("select") {
            Result::Ok(Statement::new_select_statement())
        } else if input_buffer.starts_with("select") {
            sql::parse(input_buffer.as_bytes()).map(|parsed_sql| {
                // TODO: get schema by table name
                let codes = codegen::gen_code(&parsed_sql, schema);
                Statement::new_select_statement2(parsed_sql, codes)
            })
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
                    codes: Vec::new(),
                    stack: Vec::new(),
                    sym_table: Vec::new(),
                    row_buf: RowBuf::new(),
                    pc: 0,
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
            StatementType::SELECT => {
                loop {
                    match self.execute_codes() {
                        ExecResult::Complete => break,
                        ExecResult::PendingRow => {
                            println!("{}", self.row_buf);
                        }
                        ExecResult::Error(error) => {
                            return Result::Err(format!("vm execute error: {}", error));
                        }
                    }
                }
                Result::Ok(())
            }
            StatementType::INSERT => {
                if let Some(r) = self.row_to_insert.as_ref() {
                    table.insert_cursor(r.id).save(r)
                } else {
                    Result::Ok(())
                }
            }
        }
    }

    fn execute_codes(&mut self) -> ExecResult {
        let mut pc = self.pc;
        let mut result = ExecResult::Complete;
        while pc < self.codes.len() {
            let code = &self.codes[pc];
            pc += 1;
            match code {
                &OpCode::LoadInt(i) => self.stack.push(i),
                &OpCode::Add => {
                    if let (Some(v1), Some(v2)) = (self.stack.pop(), self.stack.pop()) {
                        self.stack.push(v1 + v2);
                    } else {
                        result = ExecResult::Error("invalid state of stack.".to_owned());
                        break;
                    }
                }
                &OpCode::StoreInt => {
                    if let Some(v1) = self.stack.pop() {
                        self.row_buf.write_int(v1);
                    } else {
                        result = ExecResult::Error("invalid state of stack.".to_owned());
                        break;
                    }
                }
                &OpCode::FlushRow => {
                    // stop process of codes due to a new row
                    self.pc = pc;
                    result = ExecResult::PendingRow;
                    break;
                }
                &OpCode::LoadStr(ref str) => {
                    self.stack.push(self.sym_table.len() as i64);
                    self.sym_table.push(str.to_owned());
                }
                &OpCode::StoreStr => {
                    let len = self.sym_table.len();
                    match self.stack.pop() {
                        Some(sym_index) if (sym_index as usize) < len => {
                            let str = &self.sym_table[sym_index as usize];
                            self.row_buf.write_str(str);
                        }
                        Some(sym_index) => {
                            result = ExecResult::Error(format!(
                                "invalid symbol table index {}.",
                                sym_index
                            ));
                            break;
                        }
                        None => {
                            result = ExecResult::Error("invalid state of stack.".to_owned());
                            break;
                        }
                    }
                }
                _ => {
                    result = ExecResult::Error(format!("not implemented op code."));
                    break;
                }
            }
        }

        return result;
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    fn get_schema() -> Schema {
        Schema::new()
    }

    #[test]
    fn statement_can_prepare_for_select_1() {
        let schema = get_schema();
        let prepare_result = Statement::prepare("select 1", &schema);
        assert!(prepare_result.is_ok());
    }

    fn verify_vm_execution(sql: &str, expected: &str) {
        let schema = get_schema();
        match Statement::prepare(sql, &schema) {
            Result::Ok(mut statement) => match statement.execute_codes() {
                ExecResult::PendingRow => {
                    assert_eq!(format!("{}", statement.row_buf), expected);
                }
                _ => assert!(false, "invalid execute result!"),
            },
            _ => assert!(false, "prepare statement error"),
        }
    }

    #[test]
    fn vm_works() {
        verify_vm_execution("select 41 + 1", "(42)");
    }

    #[test]
    fn vm_can_select_text() {
        verify_vm_execution("select 'hello, rdb!'", "('hello, rdb!')");
    }

    #[test]
    fn vm_can_select_multiple_columns() {
        verify_vm_execution("select 42, 'hello, rdb!'", "(42, 'hello, rdb!')");
    }
}
