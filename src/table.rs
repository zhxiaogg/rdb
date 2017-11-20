use std::ops::{Range, Index, IndexMut};
use std::fs::{File, OpenOptions};
use byteorder::{BigEndian, ByteOrder};

const PAGE_SIZE:usize = 4096;
const MAX_PAGE_PER_TABLE:usize = 100;
const ROW_SIZE:usize = 4 + 32 + 256;

pub struct Row {
    pub id: i32,
    pub username: String,
    pub email: String
}

impl Row {
    fn serialize(row:&Row) -> Vec<u8> {
        let mut buf:Vec<u8> = vec![0;ROW_SIZE];
        BigEndian::write_i32(&mut buf.index_mut(Range{start: 0, end: 4}), row.id);
        Row::write_string(&mut buf, 4, &row.username, 32);
        Row::write_string(&mut buf, 36, &row.email, 256);
        return buf;
    }

    fn deserialize(buf: &Vec<u8>, pos: usize) -> Row {
        let mut position = pos;
        let id = BigEndian::read_i32(buf.as_slice());
        position += 4;
        let username = Row::read_string(buf, position, 32);
        position += 32;
        let email = Row::read_string(buf, position, 256);
        Row {id: id, username: username, email: email}
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
        while ((end - pos) < length) && (buf[end] != 0)  {
            end+=1;
        }
        let mut bytes = vec![0;end - pos];
        bytes.clone_from_slice(buf.index(Range{start: pos, end: end}));
        return String::from_utf8(bytes).unwrap();
    }
}

type Page = Vec<u8>;

pub struct Table {
    pager: Pager,
    pub num_rows: usize
}

impl Table {
    pub fn new(file:&str) -> Table {
        let pager = Pager::new(file);
        return Table {pager: pager, num_rows: 0};
    }

    pub fn max_rows(self:&Table) -> usize {
        return PAGE_SIZE / ROW_SIZE * MAX_PAGE_PER_TABLE;
    }

    pub fn insert(self:&mut Table, row:&Row) -> usize {
        let bytes = Row::serialize(row);
        {
            let row_index = self.num_rows;
            let (page, mut pos) = self.row_slot_for_write(row_index);
            for b in bytes {
                page[pos] = b;
                pos+=1;
            }
        }
        self.num_rows += 1;
        self.num_rows
    }

    pub fn get_row(self: &Table, row_index:usize) -> Row {
        let (page, pos) = self.row_slot_for_read(row_index);
        Row::deserialize(page, pos)
    }

    fn row_slot_for_write(self:&mut Table, row_index:usize) -> (&mut Page, usize) {
        let rows_per_page = PAGE_SIZE / ROW_SIZE;
        let page_index = row_index / rows_per_page;
        let page = self.pager.page_for_write(page_index);
        return (page, ROW_SIZE * (row_index % rows_per_page));
    }

    fn row_slot_for_read(self: &Table, row_index:usize) -> (&Page, usize) {
        let rows_per_page = PAGE_SIZE / ROW_SIZE;
        let page_index = row_index / rows_per_page;
        let page = self.pager.page_for_read(page_index);
        return (page, ROW_SIZE * (row_index % rows_per_page));
    }
}

struct Pager {
    // file: File,
    pages:Vec<Page>,
    num_pages: usize
}

impl Pager {
    fn new(file: &str) -> Pager {
        // let file = OpenOptions::new().read(true).write(true).create(true).open(file);
        // let file_size = file.metadata().unwrap().len();
        // let num_rows = file_size / ROW_SIZE;
        // let num_pages = file_size / PAGE_SIZE;
        let pages = Vec::with_capacity(MAX_PAGE_PER_TABLE);
        Pager {pages: pages, num_pages: 0}
    }

    fn page_for_read(self: &Pager, page_index: usize) -> &Page {
        &self.pages[page_index]
    }

    fn page_for_write(self: &mut Pager, page_index: usize) -> &mut Page {
        while self.pages.len() <= page_index {
            self.pages.push(vec![0; PAGE_SIZE]);
        }
        &mut self.pages[page_index]
    }
}
