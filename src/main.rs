
use std::io;
use std::process;
use std::io::Write;
use std::ops::{Range};
use std::ops::Index;
use std::ops::IndexMut;

extern crate byteorder;
use byteorder::{BigEndian, ByteOrder};

fn main() {
    //TODO: print rdb info
    let mut table = Table::new();

    let mut input_buffer = String::new();
    loop {
        print_prompt();

        read_input(&mut input_buffer);

        if input_buffer.starts_with(".") {
            match do_meta_command(&input_buffer) {
                MetaCommandResult::SUCCESS => {},
                MetaCommandResult::UNRECOGNIZED => {
                    println!("Unrecognized command: {}", input_buffer.trim());
                }
            }
            continue;
        }

        let mut statement = Statement { kind : StatementType::SELECT, row_to_insert: None };
        match prepare_statement(&input_buffer, &mut statement) {
            PrepareResult::SUCCESS => {
                execute_statement(&statement, &mut table);
                println!("Executed!");
            },
            PrepareResult::UNRECOGNIZED => {
                println!("Unrecognized command: {}", input_buffer.trim());
            },
            PrepareResult::SYNTAX_ERROR => {
                println!("Illegal syntax command: {}", input_buffer.trim());
            }
        }
    }
}

enum MetaCommandResult {
    SUCCESS,
    UNRECOGNIZED
}

fn do_meta_command(input_buffer:&str) -> MetaCommandResult {
    if input_buffer.trim().eq(".exit") {
        process::exit(0)
    } else {
        MetaCommandResult::UNRECOGNIZED
    }
}

enum StatementType {
    SELECT,
    INSERT
}

struct Row {
    id: i32,
    username: String,
    email: String
}

struct Statement {
    kind: StatementType,
    row_to_insert: Option<Row>
}

enum PrepareResult {
    SUCCESS,
    UNRECOGNIZED,
    SYNTAX_ERROR
}

const PAGE_SIZE:usize = 4096;
const MAX_PAGE_PER_TABLE:usize = 100;
const ROW_SIZE:usize = 4 + 32 + 256;

type Page = Vec<u8>;
struct Table {
    pages:Vec<Page>,
    num_rows: usize
}

impl Table {
    fn new() -> Table {
        return Table {pages: Vec::with_capacity(MAX_PAGE_PER_TABLE), num_rows: 0};
    }

    fn insert(self:&mut Table, row:&Row) {
        let bytes = Table::serialize(row);
        {
            let row_index = self.num_rows;
            let (mut page, mut pos) = self.row_slot_for_write(row_index);
            for b in bytes {
                page[pos] = b;
                pos+=1;
            }
        }
        self.num_rows += 1;
    }

    fn row_slot_for_write(self:&mut Table, rowIndex:usize) -> (&mut Page, usize) {
        let rows_per_page = PAGE_SIZE / ROW_SIZE;
        let mut page_num = rowIndex / rows_per_page;
        if self.pages.len() < page_num + 1 {
            self.pages.push(vec![0; PAGE_SIZE]);
        }
        return (&mut self.pages[page_num], ROW_SIZE * (rowIndex % rows_per_page));
    }

    fn row_slot_for_read(self: &Table, rowIndex:usize) -> (&Page, usize) {
        let rows_per_page = PAGE_SIZE / ROW_SIZE;
        let mut page_num = rowIndex / rows_per_page;
        return (&self.pages[page_num], ROW_SIZE * (rowIndex % rows_per_page));
    }

    fn serialize(row:&Row) -> Vec<u8> {
        let mut buf:Vec<u8> = vec![0;ROW_SIZE];
        BigEndian::write_i32(&mut buf.index_mut(Range{start: 0, end: 4}), row.id);
        Table::write_string(&mut buf, 4, &row.username, 32);
        Table::write_string(&mut buf, 36, &row.email, 256);
        return buf;
    }

    fn write_string(buf:&mut Vec<u8>, pos:usize, s:&str, length:usize) {
        let bytes = s.as_bytes();
        let mut vec = vec![0;bytes.len()];
        vec.copy_from_slice(bytes);

        let mut i = 0;
        for b in vec {
            buf[pos+i] = b;
            i+=1;
        }
        while i < length {
            buf[pos+i] = 0;
            i+=1;
        }
    }

    fn read_string(buf:&Vec<u8>, pos:usize, length:usize) -> String {
        let mut end = pos;
        while ((end - pos + 1) < length) && (buf[end] != 0)  {
            end+=1;
        }
        let mut bytes = vec![0;end - pos + 1];
        bytes.clone_from_slice(buf.index(Range{start: pos, end: end+1}));
        return String::from_utf8(bytes).unwrap();
    }

    fn get_row(self: &Table, row_index:usize) -> Row {
        let (page, pos) = self.row_slot_for_read(row_index);
        let mut position = pos;
        let id = BigEndian::read_i32(page.as_slice());
        position += 4;
        let username = Table::read_string(&page, position, 32);
        position += 32;
        let email = Table::read_string(&page, position, 256);

        return Row {id: id, username: username, email: email};
    }
}

fn prepare_statement(input_buffer:&str, statement:&mut Statement) -> PrepareResult {
    if input_buffer.starts_with("select") {
        statement.kind = StatementType::SELECT;
        PrepareResult::SUCCESS
    } else if input_buffer.starts_with("insert") {
        let id: i32;
        let mut username = String::new();
        let mut email = String::new();
        statement.kind = StatementType::INSERT;
        statement.row_to_insert = Some(Row {id: 1, username: String::from("username"), email: String::from("email")});
        PrepareResult::SUCCESS
    } else {
        PrepareResult::UNRECOGNIZED
    }
}

fn execute_statement(statement:&Statement, table: &mut Table) {
    match statement.kind {
        StatementType::SELECT => {
            for i in 0..table.num_rows {
                let row = table.get_row(i);
                println!("{}\t{}\t{}\t", row.id, &row.username, &row.email);
            }
        },
        StatementType::INSERT => {
            if let Some(r) = statement.row_to_insert.as_ref() {
                table.insert(r);
            }
        }
    }
}

fn print_prompt() {
    print!("rdb>");
    io::stdout().flush().unwrap();
}

fn read_input(input_buffer:&mut String) {
    input_buffer.clear();
    io::stdin().read_line(input_buffer).expect("read input error.");
}
