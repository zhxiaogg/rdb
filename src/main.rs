
use std::io;
use std::process;
use std::io::Write;

fn main() {
    //TODO: print rdb info
    let mut input_buffer = String::new();
    loop {
        print_prompt();

        read_input(&mut input_buffer);

        if input_buffer.starts_with(".") {
            match do_meta_command(&input_buffer) {
                MetaCommandResult::SUCCESS => continue;
                MetaCommandResult::UNRECOGNIZED => {
                    println!("Unrecognized command: {}", input_buffer.trim());
                    continue;
                }
            }
        }

        let mut statement = Statement { kind : StatementType::SELECT };
        match prepare_statement(&input_buffer, &mut statement) {
            PrepareResult::SUCCESS => {
                execute_statement(&statement);
                println!("Executed!");
            },
            PrepareResult::UNRECOGNIZED => {
                println!("Unrecognized command: {}", input_buffer.trim());
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
    kind: StatementType
}

enum PrepareResult {
    SUCCESS,
    UNRECOGNIZED
}

fn prepare_statement(input_buffer:&str, statement:&mut Statement) -> PrepareResult {
    if input_buffer.starts_with("select") {
        statement.kind = StatementType::SELECT;
        PrepareResult::SUCCESS
    } else if input_buffer.starts_with("insert") {
        statement.kind = StatementType::INSERT;
        PrepareResult::SUCCESS
    } else {
        PrepareResult::UNRECOGNIZED
    }
}

fn execute_statement(statement:&Statement) {
    match statement.kind {
        StatementType::SELECT => println!("this is where we do a select.", ),
        StatementType::INSERT => println!("this is where we do an insert.", ),
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
