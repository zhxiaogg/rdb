use std::ops::{Range, Index, IndexMut};
use std::fs::{File, OpenOptions};
use byteorder::{BigEndian, ByteOrder};
use std::io::{Seek, SeekFrom, Read, Write};

const PAGE_SIZE: usize = 4096;
const MAX_PAGE_PER_TABLE: usize = 100;
const ROW_SIZE: usize = 4 + 32 + 256;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;

pub struct Row {
    pub id: i32,
    pub username: String,
    pub email: String,
}

impl Row {
    fn serialize(row: &Row, page: &mut Page, pos: usize) {
        BigEndian::write_i32(
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
        let id = BigEndian::read_i32(bytes.as_slice());
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

pub struct Table {
    pager: Pager,
    pub num_rows: usize,
}

impl Table {
    pub fn new(file: &str) -> Table {
        let pager = Pager::new(file);
        let num_rows = if pager.num_pages == 0 {
            0
        } else {
            // this is a bug, the last page will lost if its rows length > 1
            (pager.num_pages - 1) * ROWS_PER_PAGE + 1
        };
        return Table {
            pager: pager,
            num_rows: num_rows,
        };
    }

    pub fn close(self: &mut Table) {
        for page_index in 0..self.pager.num_pages {
            self.pager.flush(page_index);
        }
    }

    pub fn is_full(self: &Table) -> bool {
        return self.num_rows >= PAGE_SIZE / ROW_SIZE * MAX_PAGE_PER_TABLE;
    }

    //TODO: select cursor should not pass a mutable table
    pub fn select_cursor(self: &mut Table) -> Cursor {
        Cursor::new(self, 0)
    }

    pub fn insert_cursor(self: &mut Table) -> Cursor {
        let row_index = self.num_rows;
        Cursor::new(self, row_index)
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
}

pub struct Cursor<'a> {
    table: &'a mut Table,
    curr_index: usize,
}

impl<'a> Cursor<'a> {
    fn new(table: &'a mut Table, row_index: usize) -> Cursor<'a> {
        Cursor {
            table: table,
            curr_index: row_index,
        }
    }

    pub fn end_of_table(self: &Cursor<'a>) -> bool {
        self.curr_index >= self.table.num_rows
    }

    pub fn advance(self: &mut Cursor<'a>) {
        self.curr_index += 1;
    }

    pub fn get(self: &mut Cursor<'a>) -> Row {
        let row_index = self.curr_index;
        let (page, pos) = Cursor::row_slot_for_read(self.table, row_index);
        Row::deserialize(page, pos)
    }

    pub fn save(self: &mut Cursor<'a>, row: &Row) {
        {
            let (page, pos) = Cursor::row_slot_for_write(self.table, self.curr_index);
            Row::serialize(row, page, pos);
        }
        self.table.num_rows += 1;
    }

    fn row_slot_for_write(table: &mut Table, row_index: usize) -> (&mut Page, usize) {
        let page_index = row_index / ROWS_PER_PAGE;
        let page = table.pager.page_for_write(page_index);
        return (page, ROW_SIZE * (row_index % ROWS_PER_PAGE));
    }

    fn row_slot_for_read(table: &mut Table, row_index: usize) -> (&Page, usize) {
        let page_index = row_index / ROWS_PER_PAGE;
        let page = table.pager.page_for_read(page_index);
        return (page, ROW_SIZE * (row_index % ROWS_PER_PAGE));
    }
}
