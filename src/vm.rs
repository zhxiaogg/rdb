use std::ops::{Index, IndexMut, RangeFrom};
use std::usize;
use std::cmp;
use byteorder::{BigEndian, ByteOrder};

use table::{Row, Table};
use sql;
use sql::ParsedSQL;
use codegen;
use codegen::{OpCode, SQLType};

pub enum StatementType {
    SELECT,
    INSERT,
}

pub struct RowBuf {
    buf: Vec<u8>,
    num_columns: usize,
    column_types: Vec<SQLType>,
    buf_index: usize,
}

impl RowBuf {
    fn new() -> RowBuf {
        RowBuf {
            buf: vec![0u8; 512],
            num_columns: 0,
            column_types: Vec::new(),
            buf_index: 0,
        }
    }
    pub fn reset(&mut self) {
        self.num_columns = 0;
        self.column_types.clear();
        self.buf_index = 0;
    }

    pub fn write_int(&mut self, i: i64) {
        let column_size = codegen::size_of(SQLType::Integer);

        self.num_columns += 1;
        self.column_types.push(SQLType::Integer);
        let size_demand = self.buf_index + column_size;
        let capacity = self.buf.len();
        if size_demand > capacity {
            self.buf.resize(cmp::max(size_demand, capacity * 2), 0u8);
        }
        BigEndian::write_i64(
            self.buf.index_mut(RangeFrom {
                start: self.buf_index,
            }),
            i,
        );
        self.buf_index += column_size;
    }

    pub fn read_int(&self, column_index: usize) -> i64 {
        let mut offset = 0;
        for i in 0..column_index {
            offset += codegen::size_of(self.column_types[i]);
        }
        BigEndian::read_i64(self.buf.index(RangeFrom { start: offset }))
    }
}

pub struct Statement {
    kind: StatementType,
    parsed: Option<ParsedSQL>,
    codes: Vec<OpCode>,
    row_to_insert: Option<Row>,
    stack: Vec<i64>,
    pub row_buf: RowBuf,
    pc: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ExecResult {
    PendingRow,
    Complete,
}

pub trait VM {
    fn execute(&mut self, table: &mut Table) -> Result<ExecResult, String>;
    fn execute_codes(&mut self) -> Result<ExecResult, String>;
}

impl Statement {
    fn new_select_statement() -> Statement {
        Statement {
            kind: StatementType::SELECT,
            row_to_insert: None,
            parsed: None,
            codes: Vec::new(),
            stack: Vec::new(),
            row_buf: RowBuf::new(),
            pc: usize::MAX,
        }
    }

    fn new_select_statement2(parsed_sql: ParsedSQL, codes: Vec<OpCode>) -> Statement {
        Statement {
            kind: StatementType::SELECT,
            row_to_insert: None,
            parsed: Some(parsed_sql),
            codes: codes,
            stack: Vec::new(),
            row_buf: RowBuf::new(),
            pc: usize::MAX,
        }
    }

    pub fn prepare(input_buffer: &str) -> Result<Statement, String> {
        if input_buffer.eq("select") {
            Result::Ok(Statement::new_select_statement())
        } else if input_buffer.starts_with("select") {
            sql::parse(input_buffer.as_bytes()).map(|parsed_sql| {
                let codes = codegen::gen_code(&parsed_sql);
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
                    row_buf: RowBuf::new(),
                    pc: usize::MAX,
                };
                Result::Ok(statement)
            }
        } else {
            Result::Err(format!("Unrecognized command: {}", input_buffer).to_owned())
        }
    }
}

impl VM for Statement {
    fn execute(&mut self, table: &mut Table) -> Result<ExecResult, String> {
        match self.kind {
            StatementType::SELECT if self.parsed.is_none() => {
                let mut cursor = table.select_cursor();
                while !cursor.end_of_table() {
                    let row = cursor.get();
                    println!("({}, {}, {})", row.id, &row.username, &row.email);
                    cursor.advance();
                }
                Result::Ok(ExecResult::Complete)
            }
            StatementType::SELECT => self.execute_codes(),
            StatementType::INSERT => {
                if let Some(r) = self.row_to_insert.as_ref() {
                    table
                        .insert_cursor(r.id)
                        .save(r)
                        .map(|_| ExecResult::Complete)
                } else {
                    Result::Ok(ExecResult::Complete)
                }
            }
        }
    }

    fn execute_codes(&mut self) -> Result<ExecResult, String> {
        let mut pc = 0;
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
                        return Result::Err("invalid byte codes1.".to_owned());
                    }
                }
                &OpCode::StoreInt => {
                    if let Some(v1) = self.stack.pop() {
                        self.row_buf.write_int(v1);
                    } else {
                        return Result::Err("store int: invalid byte codes.".to_owned());
                    }
                }
                &OpCode::FlushRow => {
                    // stop process of codes due to a new row
                    self.pc = pc;
                    result = ExecResult::PendingRow;
                    break;
                }
                _ => {}
            }
        }

        return Result::Ok(result);
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

    #[test]
    fn can_read_integer_from_row_buf() {
        let mut row_buf = RowBuf::new();
        row_buf.write_int(42);
        assert_eq!(row_buf.read_int(0), 42);
    }

    #[test]
    fn can_read_all_column_values_from_row_buf() {
        let mut row_buf = RowBuf::new();
        for i in 0..100 {
            row_buf.write_int(i * 10);
        }

        for i in 0..100 {
            assert_eq!(row_buf.read_int(i), (i * 10) as i64);
        }
    }

    #[test]
    fn vm_works() {
        match Statement::prepare("select 41 + 1") {
            Result::Ok(mut statement) => match statement.execute_codes() {
                Result::Ok(r) => {
                    assert_eq!(r, ExecResult::PendingRow);
                    assert_eq!(statement.row_buf.read_int(0), 42);
                }
                Result::Err(e) => assert!(false, e),
            },
            _ => assert!(false, "prepare statement error"),
        }
    }
}
