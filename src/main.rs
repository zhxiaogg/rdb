
use std::io;
use std::process;
use std::io::Write;

fn main() {
    //TODO: print rdb info
    let mut input_buffer = String::new();
    loop {
        print_prompt();

        read_input(&mut input_buffer);

        if input_buffer.trim().eq(".exit") {
            process::exit(0);
        } else {
            println!("Unrecognized command {}", input_buffer.trim());
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
