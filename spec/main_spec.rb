describe 'database' do
  before do
    `rm -rf test.rdb`
  end

  def run_script(commands)
    raw_output = nil
    IO.popen("./target/debug/rdb test.rdb", "r+") do |pipe|
      commands.each do |command|
        pipe.puts command
      end

      pipe.close_write

      # Read entire output
      raw_output = pipe.gets(nil)
    end
    raw_output.split("\n")
  end

  it 'inserts and retreives a row' do
    result = run_script([
      "insert 1 user1 person1@example.com",
      "select",
      ".exit",
    ])
    expect(result).to eq([
      "rdb > Executed.",
      "rdb > (1, user1, person1@example.com)",
      "Executed.",
      "rdb > ",
    ])
  end

  # it 'prints error message when table is full' do
  #   script = (1..1401).map do |i|
  #     "insert #{i} user#{i} person#{i}@example.com"
  #   end
  #   script << ".exit"
  #   result = run_script(script)
  #   expect(result[-2]).to eq('rdb > Error: Table full.')
  # end

  it 'allows inserting strings that are the maximum length' do
    long_username = "a"*32
    long_email = "a"*255
    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to eq([
      "rdb > Executed.",
      "rdb > (1, #{long_username}, #{long_email})",
      "Executed.",
      "rdb > ",
    ])
  end

  it 'prints error message if strings are too long' do
    long_username = "a"*33
    long_email = "a"*256
    script = [
      "insert 1 #{long_username} #{long_email}",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to eq([
      "rdb > String is too long.",
      "rdb > Executed.",
      "rdb > ",
    ])
  end

  it 'prints an error message if id is negative' do
    script = [
      "insert -1 cstack foo@bar.com",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to eq([
      "rdb > ID must be positive.",
      "rdb > Executed.",
      "rdb > ",
    ])
  end

  it 'keeps data after closing connection' do
    result1 = run_script([
      "insert 1 user1 person1@example.com",
      ".exit",
    ])
    expect(result1).to eq([
      "rdb > Executed.",
      "rdb > ",
    ])
    result2 = run_script([
      "select",
      ".exit",
    ])
    expect(result2).to eq([
      "rdb > (1, user1, person1@example.com)",
      "Executed.",
      "rdb > ",
    ])
  end

  it 'prints an error message if there is a duplicate id' do
    script = [
      "insert 1 user1 person1@example.com",
      "insert 1 user1 person1@example.com",
      "select",
      ".exit",
    ]
    result = run_script(script)
    expect(result).to eq([
      "rdb > Executed.",
      "rdb > Error: Duplicate key.",
      "rdb > (1, user1, person1@example.com)",
      "Executed.",
      "rdb > ",
    ])
  end

  it 'allows printing out the structure of a one-node btree' do
    script = [3, 1, 2].map do |i|
      "insert #{i} user#{i} person#{i}@example.com"
    end
    script << ".btree"
    script << ".exit"
    result = run_script(script)

    expect(result).to eq([
      "rdb > Executed.",
      "rdb > Executed.",
      "rdb > Executed.",
      "rdb > Tree:",
      "leaf (size 3)",
      "  - 0 : 1",
      "  - 1 : 2",
      "  - 2 : 3",
      "rdb > "
    ])
  end

  it 'prints constants' do
    script = [
      ".constants",
      ".exit",
    ]
    result = run_script(script)

    expect(result).to eq([
      "rdb > Constants:",
      "ROW_SIZE: 292",
      "COMMON_NODE_HEADER_SIZE: 6",
      "LEAF_NODE_HEADER_SIZE: 10",
      "LEAF_NODE_CELL_SIZE: 296",
      "LEAF_NODE_SPACE_FOR_CELLS: 4086",
      "LEAF_NODE_MAX_CELLS: 13",
      "rdb > ",
    ])
  end
end
