extern crate byteorder;

use std::io;
use std::process;
use std::io::Write;

mod table;
use table::{Table, Row};

fn main() {
    //TODO: print rdb info
    let mut table = Table::new("");

    let mut input_buffer = String::new();
    loop {
        print_prompt();

        read_input(&mut input_buffer);

        if input_buffer.starts_with(".") {
            match do_meta_command(&input_buffer) {
                MetaCommandResult::Success => {},
                MetaCommandResult::UNRECOGNIZED => {
                    println!("Unrecognized command: {}", input_buffer.trim());
                }
            }
            continue;
        }

        let mut statement = Statement { kind : StatementType::SELECT, row_to_insert: None };
        match prepare_statement(&input_buffer, &mut statement) {
            PrepareResult::Success => {
                match execute_statement(&statement, &mut table) {
                    ExecuteResult::Success => println!("Executed."),
                    ExecuteResult::TableFull => println!("Error: Table full.")
                }
            },
            PrepareResult::UNRECOGNIZED => {
                println!("Unrecognized command: {}", input_buffer.trim());
            },
            PrepareResult::SyntaxError(message) => {
                println!("{}", &message);
            }
        }
    }
}

enum MetaCommandResult {
    Success,
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
    Success,
    UNRECOGNIZED,
    SyntaxError(String)
}

enum ExecuteResult {
    Success,
    TableFull
}


fn prepare_statement(input_buffer:&str, statement:&mut Statement) -> PrepareResult {
    if input_buffer.starts_with("select") {
        statement.kind = StatementType::SELECT;
        PrepareResult::Success
    } else if input_buffer.starts_with("insert") {

        let parts:Vec<&str> = input_buffer.trim().splitn(4, ' ').collect();
        if parts.len() != 4 {
            PrepareResult::SyntaxError(input_buffer.to_owned())
        } else {
            let id = i32::from_str_radix(parts[1], 10).unwrap();
            if id < 0 {
                return PrepareResult::SyntaxError("ID must be positive.".to_owned())
            }
            let username = String::from(parts[2]);
            let email = String::from(parts[3]);
            if username.len() > 32 || email.len() > 256 {
                return PrepareResult::SyntaxError("String is too long.".to_owned())
            }
            statement.kind = StatementType::INSERT;
            statement.row_to_insert = Some(Row {id: id, username: username, email: email});
            PrepareResult::Success
        }
    } else {
        PrepareResult::UNRECOGNIZED
    }
}

fn execute_statement(statement:&Statement, table: &mut Table) -> ExecuteResult {
    match statement.kind {
        StatementType::SELECT => {
            for i in 0..table.num_rows {
                let row = table.get_row(i);
                println!("({}, {}, {})", row.id, &row.username, &row.email);
            }
            ExecuteResult::Success
        },
        StatementType::INSERT => {
            if table.num_rows >= table.max_rows() {
                ExecuteResult::TableFull
            } else if let Some(r) = statement.row_to_insert.as_ref() {
                table.insert(r);
                ExecuteResult::Success
            } else {
                ExecuteResult::Success
            }
        }
    }
}

fn print_prompt() {
    print!("rdb > ");
    io::stdout().flush().unwrap();
}

fn read_input(input_buffer:&mut String) {
    input_buffer.clear();
    io::stdin().read_line(input_buffer).expect("read input error.");
}
