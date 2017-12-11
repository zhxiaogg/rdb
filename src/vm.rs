use std::ops::{Index, IndexMut, Range, RangeFrom};
use std::cmp;
use std::fmt;
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
    column_types: Vec<SQLType>,
    buf_index: usize,
}

impl RowBuf {
    fn new() -> RowBuf {
        RowBuf {
            buf: vec![0u8; 512],
            column_types: Vec::new(),
            buf_index: 0,
        }
    }
    pub fn reset(&mut self) {
        self.column_types.clear();
        self.buf_index = 0;
    }

    fn resize(&mut self, size_demand: usize) {
        let size_required = self.buf_index + size_demand;
        let capacity = self.buf.len();
        if size_required > capacity {
            self.buf.resize(cmp::max(size_required, capacity * 2), 0u8);
        }
    }

    fn column_offset(&self, column_index: usize) -> Result<usize, String> {
        if column_index >= self.column_types.len() {
            return Result::Err(format!("column index {} overflow.", column_index));
        }
        let mut offset = 0;
        for i in 0..column_index {
            let mut column_size = codegen::size_of(self.column_types[i]);
            // check if this column is variable length encoded
            if column_size == 0 {
                column_size =
                    4 + BigEndian::read_u32(self.buf.index(RangeFrom { start: offset })) as usize;
            }
            offset += column_size;
        }
        Result::Ok(offset)
    }

    pub fn write_int(&mut self, value: i64) {
        let column_size = codegen::size_of(SQLType::Integer);
        self.column_types.push(SQLType::Integer);
        self.resize(column_size);
        BigEndian::write_i64(
            self.buf.index_mut(RangeFrom {
                start: self.buf_index,
            }),
            value,
        );
        self.buf_index += column_size;
    }

    pub fn read_int(&self, column_index: usize) -> Result<i64, String> {
        self.column_offset(column_index)
            .map(|offset| BigEndian::read_i64(self.buf.index(RangeFrom { start: offset })))
    }

    pub fn write_str(&mut self, value: &str) {
        let bytes = value.as_bytes();
        let num_bytes = bytes.len();
        self.column_types.push(SQLType::String);
        self.resize(num_bytes + 4);

        BigEndian::write_u32(
            self.buf.index_mut(RangeFrom {
                start: self.buf_index,
            }),
            num_bytes as u32,
        );
        self.buf_index += 4;
        let mut index = self.buf_index;
        for b in bytes {
            self.buf[index] = *b;
            index += 1;
        }
        self.buf_index = index;
    }

    pub fn read_str(&self, column_index: usize) -> Result<String, String> {
        self.column_offset(column_index).and_then(|offset| {
            let num_bytes =
                BigEndian::read_u32(self.buf.index(RangeFrom { start: offset })) as usize;
            let bytes = self.buf.index(Range {
                start: offset + 4,
                end: offset + 4 + num_bytes,
            });
            String::from_utf8(bytes.to_vec()).map_err(|_| "invalid utf8 bytes.".to_owned())
        })
    }
}

impl fmt::Display for RowBuf {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let num_columns = self.column_types.len();

        let mut line = format!("(");
        for column_index in 0..num_columns {
            if column_index > 0 {
                line = format!("{}, ", line);
            }
            match self.column_types[column_index] {
                SQLType::Integer => match self.read_int(column_index) {
                    Result::Ok(v) => {
                        line = format!("{}{}", line, v);
                    }
                    Result::Err(str) => {
                        line = format!("{}{}", line, &str);
                        break;
                    }
                },
                SQLType::String => match self.read_str(column_index) {
                    Result::Ok(str) => {
                        line = format!("{}'{}'", line, &str);
                    }
                    Result::Err(str) => {
                        line = format!("{}{}", line, &str);
                        break;
                    }
                },
            }
        }
        line = format!("{})", line);
        write!(f, "{}", line)
    }
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

    #[test]
    fn statement_can_prepare_for_select_1() {
        let prepare_result = Statement::prepare("select 1");
        assert!(prepare_result.is_ok());
    }

    #[test]
    fn can_read_integer_from_row_buf() {
        let mut row_buf = RowBuf::new();
        row_buf.write_int(42);
        assert_eq!(row_buf.read_int(0), Result::Ok(42));
    }

    #[test]
    fn can_read_all_column_values_from_row_buf() {
        let mut row_buf = RowBuf::new();

        row_buf.write_str("foo");
        for i in 0..100 {
            row_buf.write_int(i * 10);
        }
        row_buf.write_str("bar");

        assert_eq!(row_buf.read_str(0), Result::Ok("foo".to_owned()));
        for i in 0..100 {
            assert_eq!(row_buf.read_int(i + 1), Result::Ok((i * 10) as i64));
        }
        assert_eq!(row_buf.read_str(101), Result::Ok("bar".to_owned()));
    }

    #[test]
    fn can_read_string_from_row_buf() {
        let mut row_buf = RowBuf::new();
        row_buf.write_str("rdb");
        assert_eq!(row_buf.read_str(0), Result::Ok("rdb".to_owned()));
    }

    #[test]
    fn vm_works() {
        match Statement::prepare("select 41 + 1") {
            Result::Ok(mut statement) => match statement.execute_codes() {
                ExecResult::PendingRow => {
                    assert_eq!(statement.row_buf.read_int(0), Result::Ok(42));
                }
                _ => assert!(false, "invalid execute result!"),
            },
            _ => assert!(false, "prepare statement error"),
        }
    }

    #[test]
    fn vm_can_select_text() {
        match Statement::prepare("select 'hello, rdb!'") {
            Result::Ok(mut statement) => match statement.execute_codes() {
                ExecResult::PendingRow => {
                    assert_eq!(
                        statement.row_buf.read_str(0),
                        Result::Ok("hello, rdb!".to_owned())
                    );
                }
                _ => assert!(false, "invalid execute result!"),
            },
            _ => assert!(false, "prepare statement error"),
        }
    }
}
