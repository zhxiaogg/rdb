extern crate byteorder;
extern crate regex;

use std::io;
use std::process;
use std::io::Write;

use regex::Regex;

mod table;
use table::{Table, Row};

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

struct Statement {
    kind: StatementType,
    row_to_insert: Option<Row>
}

enum PrepareResult {
    SUCCESS,
    UNRECOGNIZED,
    SYNTAX_ERROR
}

fn prepare_statement(input_buffer:&str, statement:&mut Statement) -> PrepareResult {
    if input_buffer.starts_with("select") {
        statement.kind = StatementType::SELECT;
        PrepareResult::SUCCESS
    } else if input_buffer.starts_with("insert") {
        let re = Regex::new(r"^insert[\s\t]+(\d+)[\s\t]+(\w+)[\s\t]+([\w@\.\d]+)[\s\t]*$").unwrap();
        if !re.is_match(input_buffer) {
            PrepareResult::SYNTAX_ERROR
        } else {
            let captures = re.captures(input_buffer).unwrap();
            let id = i32::from_str_radix(captures.get(1).unwrap().as_str(), 10).unwrap();
            let username = String::from(captures.get(2).unwrap().as_str());
            let email = String::from(captures.get(3).unwrap().as_str());
            statement.kind = StatementType::INSERT;
            statement.row_to_insert = Some(Row {id: id, username: username, email: email});
            PrepareResult::SUCCESS
        }
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
