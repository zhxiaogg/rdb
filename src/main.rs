extern crate byteorder;

use std::io;
use std::process;
use std::io::Write;
use std::env;

mod table;
use table::{Table, Row};

fn main() {
    let db = if let Some(file) = env::args().nth(1) {
        file
    } else {
        String::from("default.rdb")
    };

    //TODO: print rdb info
    let mut table = Table::new(db.as_str());

    let mut input_buffer = String::new();
    loop {
        print_prompt();

        read_input(&mut input_buffer);

        if input_buffer.starts_with(".") {
            match do_meta_command(&input_buffer.trim(), &mut table) {
                ExecuteResult::Ok => {}
                ExecuteResult::Err(msg) => {
                    println!("{}", &msg);
                }
            }
            continue;
        }

        match prepare_statement(&input_buffer.trim()) {
            PrepareResult::Ok(statement) => {
                match execute_statement(&statement, &mut table) {
                    ExecuteResult::Ok => println!("Executed."),
                    ExecuteResult::Err(msg) => println!("{}", &msg),
                }
            }
            PrepareResult::Err(msg) => {
                println!("{}", &msg);
            }
        }
    }
}

fn do_meta_command(input_buffer: &str, table: &mut Table) -> ExecuteResult {
    if input_buffer.eq(".exit") {
        table.close();
        process::exit(0)
    } else {
        ExecuteResult::Err(format!("Unrecognized command: {}", input_buffer))
    }
}

enum StatementType {
    SELECT,
    INSERT,
}

struct Statement {
    kind: StatementType,
    row_to_insert: Option<Row>,
}

enum PrepareResult {
    Ok(Statement),
    Err(String),
}

enum ExecuteResult {
    Ok,
    Err(String),
}


fn prepare_statement(input_buffer: &str) -> PrepareResult {
    if input_buffer.starts_with("select") {
        PrepareResult::Ok(Statement{kind:StatementType::SELECT, row_to_insert: None})
    } else if input_buffer.starts_with("insert") {

        let parts: Vec<&str> = input_buffer.trim().splitn(4, ' ').collect();
        if parts.len() != 4 {
            PrepareResult::Err(input_buffer.to_owned())
        } else {
            let id = i32::from_str_radix(parts[1], 10).unwrap();
            if id < 0 {
                return PrepareResult::Err("ID must be positive.".to_owned());
            }
            let username = String::from(parts[2]);
            let email = String::from(parts[3]);
            if username.len() > 32 || email.len() > 256 {
                return PrepareResult::Err("String is too long.".to_owned());
            }
            let statement = Statement {
                kind: StatementType::INSERT,
                row_to_insert: Some(Row {
                id: id,
                username: username,
                email: email,
            })};
            PrepareResult::Ok(statement)
        }
    } else {
        PrepareResult::Err(format!("Unrecognized command: {}", input_buffer).to_owned())
    }
}

fn execute_statement(statement: &Statement, table: &mut Table) -> ExecuteResult {
    match statement.kind {
        StatementType::SELECT => {
            for i in 0..table.num_rows {
                let row = table.get_row(i);
                println!("({}, {}, {})", row.id, &row.username, &row.email);
            }
            ExecuteResult::Ok
        }
        StatementType::INSERT => {
            if table.num_rows >= table.max_rows() {
                ExecuteResult::Err("Error: Table full.".to_owned())
            } else if let Some(r) = statement.row_to_insert.as_ref() {
                table.insert(r);
                ExecuteResult::Ok
            } else {
                ExecuteResult::Ok
            }
        }
    }
}

fn print_prompt() {
    print!("rdb > ");
    io::stdout().flush().unwrap();
}

fn read_input(input_buffer: &mut String) {
    input_buffer.clear();
    io::stdin().read_line(input_buffer).expect(
        "read input error.",
    );
}
