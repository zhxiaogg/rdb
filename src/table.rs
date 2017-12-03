use std::ops::{Range, RangeFrom, Index, IndexMut};
use std::fs::{File, OpenOptions};
use byteorder::{BigEndian, ByteOrder};
use std::io::{Seek, SeekFrom, Read, Write};

const PAGE_SIZE: usize = 4096;
const MAX_PAGE_PER_TABLE: usize = 100;
const ROW_SIZE: usize = 4 + 32 + 256;

const NODE_TYPE_SIZE: usize = 1;
const IS_ROOT_SIZE: usize = 1;
const PARENT_POINTER_SIZE: usize = 4;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

const NUM_CELLS_OFFSET: usize = 2;
const NUM_CELLS_SIZE: usize = 4;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + NUM_CELLS_SIZE;

const CELL_OFFSET: usize = LEAF_NODE_HEADER_SIZE;
const CELL_KEY_SIZE: usize = 4;
const CELL_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_CELL_SIZE: usize = CELL_KEY_SIZE + CELL_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

pub fn print_constants() {
    println!("Constants:");
    println!("ROW_SIZE: {}", ROW_SIZE);
    println!("COMMON_NODE_HEADER_SIZE: {}", COMMON_NODE_HEADER_SIZE);
    println!("LEAF_NODE_HEADER_SIZE: {}", LEAF_NODE_HEADER_SIZE);
    println!("LEAF_NODE_CELL_SIZE: {}", LEAF_NODE_CELL_SIZE);
    println!("LEAF_NODE_SPACE_FOR_CELLS: {}", LEAF_NODE_SPACE_FOR_CELLS);
    println!("LEAF_NODE_MAX_CELLS: {}", LEAF_NODE_MAX_CELLS);
}

pub struct Row {
    pub id: u32,
    pub username: String,
    pub email: String,
}

impl Row {
    fn serialize(row: &Row, page: &mut Page, pos: usize) {
        BigEndian::write_u32(
            page.index_mut(Range {
                start: pos,
                end: pos + 4,
            }),
            row.id,
        );
        Row::write_string(page, pos + 4, &row.username, 32);
        Row::write_string(page, pos + 36, &row.email, 256);
    }

    fn deserialize(buf: &Vec<u8>, pos: usize) -> Row {
        let mut bytes = vec![0; ROW_SIZE];
        bytes.clone_from_slice(buf.index(Range {
            start: pos,
            end: pos + ROW_SIZE,
        }));

        let mut position = 0;
        let id = BigEndian::read_u32(bytes.as_slice());
        position += 4;
        let username = Row::read_string(&bytes, position, 32);
        position += 32;
        let email = Row::read_string(&bytes, position, 256);
        Row {
            id: id,
            username: username,
            email: email,
        }
    }

    fn write_string(buf: &mut Vec<u8>, pos: usize, s: &str, length: usize) {
        let bytes = s.as_bytes();
        let mut vec = vec![0; bytes.len()];
        vec.copy_from_slice(bytes);

        let mut i = 0;
        for b in vec {
            buf[pos + i] = b;
            i += 1;
        }
        while i < length {
            buf[pos + i] = 0;
            i += 1;
        }
    }

    fn read_string(buf: &Vec<u8>, pos: usize, length: usize) -> String {
        let mut end = pos;
        while ((end - pos) < length) && (buf[end] != 0) {
            end += 1;
        }
        let mut bytes = vec![0; end - pos];
        bytes.clone_from_slice(buf.index(Range {
            start: pos,
            end: end,
        }));
        return String::from_utf8(bytes).unwrap();
    }
}

type Page = Vec<u8>;

trait PageTrait {
    fn new() -> Page;

    fn pos_for_cell(cell_index: usize) -> usize;

    fn num_cells(&self) -> u32;

    fn cell_key(&self, cell_index: usize) -> u32;

    fn set_num_cells(&mut self, num_cells: u32);

    fn print(&self);
}

impl PageTrait for Page {
    fn new() -> Page {
        let mut page = vec![0; PAGE_SIZE];
        page.set_num_cells(0);
        page
    }

    fn pos_for_cell(cell_index: usize) -> usize {
        CELL_OFFSET + cell_index * LEAF_NODE_CELL_SIZE
    }

    fn num_cells(self: &Page) -> u32 {
        BigEndian::read_u32(self.index(RangeFrom { start: NUM_CELLS_OFFSET }))
    }

    fn cell_key(self: &Page, cell_index: usize) -> u32 {
        let pos = Page::pos_for_cell(cell_index);
        BigEndian::read_u32(self.index(RangeFrom { start: pos }))
    }

    fn set_num_cells(&mut self, num_cells: u32) {
        BigEndian::write_u32(
            self.index_mut(RangeFrom { start: NUM_CELLS_OFFSET }),
            num_cells,
        )
    }

    fn print(&self) {
        let num_cells = self.num_cells();
        println!("leaf (size {})", num_cells);
        for cell_index in 0..(num_cells as usize) {
            println!("  - {} : {}", cell_index, self.cell_key(cell_index));
        }
    }
}

pub struct Table {
    pager: Pager,
    root_page_index: usize,
}

impl Table {
    pub fn new(file: &str) -> Table {
        let pager = Pager::new(file);
        return Table {
            pager: pager,
            root_page_index: 0,
        };
    }

    pub fn close(self: &mut Table) {
        for page_index in 0..self.pager.num_pages {
            self.pager.flush(page_index);
        }
    }

    pub fn is_full(self: &Table) -> bool {
        return false;
    }

    //TODO: select cursor should not pass a mutable table
    pub fn select_cursor(self: &mut Table) -> Cursor {
        let page_index = self.root_page_index;
        Cursor::new(self, page_index, 0)
    }

    pub fn insert_cursor(self: &mut Table) -> Cursor {
        let page_index = self.root_page_index;
        let cell_index = if self.pager.num_pages == 0 {
            0
        } else {
            self.pager.page_for_read(page_index).num_cells()
        };
        Cursor::new(self, page_index, cell_index as usize)
    }

    pub fn debug_index(&mut self) {
        self.pager.print();
    }
}

struct Pager {
    file: File,
    pages: Vec<Option<Page>>,
    num_pages: usize,
}

impl Pager {
    fn new(file: &str) -> Pager {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)
            .unwrap();
        let file_size = file.metadata().unwrap().len();
        let num_pages = (file_size / (PAGE_SIZE as u64)) as usize;
        let pages = vec![None; MAX_PAGE_PER_TABLE];
        Pager {
            file: file,
            pages: pages,
            num_pages: num_pages,
        }
    }

    fn flush(self: &mut Pager, page_index: usize) {
        let offset = page_index * PAGE_SIZE;
        if let Some(page) = self.pages[page_index].as_ref() {
            self.file.seek(SeekFrom::Start(offset as u64)).unwrap();
            self.file.write_all(page.as_ref()).unwrap();
        }
    }

    fn load(self: &mut Pager, page_index: usize) {
        let offset = page_index * PAGE_SIZE;
        self.file.seek(SeekFrom::Start(offset as u64)).unwrap();
        let mut buf = vec![0; PAGE_SIZE];
        self.file.read(buf.as_mut_slice()).unwrap();
        self.pages[page_index] = Some(buf);
    }

    // TODO: retreiving of a readable page should not pass a mutable pager
    fn page_for_read(self: &mut Pager, page_index: usize) -> &Page {
        if page_index >= self.num_pages {
            panic!("read EOF");
        } else if let None = self.pages[page_index] {
            self.load(page_index);
        }
        self.pages[page_index].as_ref().unwrap()
    }

    fn page_for_write(self: &mut Pager, page_index: usize) -> &mut Page {
        if page_index > self.num_pages {
            panic!("skipped write to a page");
        } else if page_index == self.num_pages {
            // need a new page
            self.pages[page_index] = Some(vec![0; PAGE_SIZE]);
            self.num_pages += 1;
        } else if let None = self.pages[page_index] {
            // read page from file
            self.load(page_index);
        }
        self.pages[page_index].as_mut().unwrap()
    }

    pub fn print(&mut self) {
        println!("Tree:");
        for page_index in 0..self.num_pages {
            let page = self.page_for_read(page_index);
            page.print();
        }
    }
}

pub struct Cursor<'a> {
    table: &'a mut Table,
    page_index: usize,
    cell_index: usize,
}

impl<'a> Cursor<'a> {
    fn new(table: &'a mut Table, page_index: usize, cell_index: usize) -> Cursor<'a> {
        Cursor {
            table: table,
            page_index: page_index,
            cell_index: cell_index,
        }
    }

    pub fn end_of_table(self: &mut Cursor<'a>) -> bool {
        let page_index = self.page_index;
        self.table.pager.num_pages == 0 ||
            (self.cell_index >= self.table.pager.page_for_read(page_index).num_cells() as usize)
    }

    pub fn advance(self: &mut Cursor<'a>) {
        self.cell_index += 1;
    }

    pub fn get(self: &mut Cursor<'a>) -> Row {
        let cell_index = self.cell_index;
        let page_index = self.page_index;
        let page = self.table.pager.page_for_read(page_index);
        let cell_pos = Page::pos_for_cell(cell_index);
        Row::deserialize(page, cell_pos + CELL_KEY_SIZE)
    }

    pub fn save(self: &mut Cursor<'a>, key: u32, row: &Row) {
        let page_index = self.page_index;
        let cell_index = self.cell_index;
        let page = self.table.pager.page_for_write(page_index);
        let cell_pos = Page::pos_for_cell(cell_index);

        BigEndian::write_u32(page.index_mut(RangeFrom { start: cell_pos }), key);
        Row::serialize(row, page, cell_pos + CELL_KEY_SIZE);
        let num_cells = page.num_cells();
        page.set_num_cells(num_cells + 1);
    }
}
