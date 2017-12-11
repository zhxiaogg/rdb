use std::ops::{Index, IndexMut, Range, RangeFrom};
use byteorder::{BigEndian, ByteOrder};
use sql::SQLType;
use std::cmp;
use std::fmt;
use super::codegen::OpCode;
use super::codegen;

pub struct RowBuf {
    buf: Vec<u8>,
    column_types: Vec<SQLType>,
    buf_index: usize,
}

impl RowBuf {
    pub fn new() -> RowBuf {
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

#[cfg(test)]
mod tests {
    use super::*;



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
}
