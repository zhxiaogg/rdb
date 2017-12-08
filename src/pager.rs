use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::{Index, IndexMut, Range, RangeFrom};
use std::cell::RefCell;
use std::rc::Rc;
use std::collections::HashMap;

use byteorder::{BigEndian, ByteOrder};

pub const DB_HEADER_SIZE: usize = 100;
// pub const DB_VERSION_OFFSET: usize = 0;
// pub const DB_VERSION_SIZE: usize = 4;
pub const DB_PAGE_SIZE_OFFSET: usize = 0;
// pub const DB_PAGE_SIZE_SIZE: usize = 4;

pub struct DbOption {
    pub page_size: usize,
}

pub type Page = Vec<u8>;
pub trait PageTrait {
    fn new_page(page_size: usize) -> Page;

    fn move_slice_internally(&mut self, from: usize, to: usize, len: usize);

    fn wrap_slice(&mut self, from: usize, buf: &Vec<u8>);
}

impl PageTrait for Page {
    fn new_page(page_size: usize) -> Page {
        vec![0u8; page_size]
    }

    fn move_slice_internally(&mut self, from: usize, to: usize, len: usize) {
        let mut vec = vec![0; len];
        {
            let slice = self.index(Range {
                start: from,
                end: from + len,
            }).clone();
            vec.clone_from_slice(slice);
        }
        let mut i = 0;
        for b in vec {
            self[to + i] = b;
            i += 1;
        }
    }

    fn wrap_slice(&mut self, from: usize, buf: &Vec<u8>) {
        let mut i = 0;
        for b in buf {
            self[from + i] = *b;
            i += 1;
        }
    }
}

pub struct Pager {
    file: RefCell<File>,
    pages: RefCell<HashMap<usize, Rc<RefCell<Page>>>>,
    pub num_pages: usize,
    db_option: DbOption,
}

impl Pager {
    pub fn new(file: &str, mut db_option: DbOption) -> Pager {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)
            .unwrap();

        let file_size = file.metadata().unwrap().len();

        if file_size > 0 {
            Pager::read_db_options(&mut file, &mut db_option);
            if Pager::is_db_corrupted(file_size, db_option.page_size) {
                panic!("db file is corrupted.");
            }
        } else {
            Pager::persist_db_options(&mut file, &db_option);
        }

        let num_pages = if file_size > 0 {
            ((file_size - DB_HEADER_SIZE as u64) / (db_option.page_size as u64)) as usize
        } else {
            0
        };
        Pager {
            file: RefCell::new(file),
            pages: RefCell::new(HashMap::new()),
            num_pages: num_pages,
            db_option: db_option,
        }
    }

    fn is_db_corrupted(file_size: u64, page_size: usize) -> bool {
        file_size < DB_HEADER_SIZE as u64
            || (file_size - DB_HEADER_SIZE as u64) % (page_size as u64) != 0
    }

    fn persist_db_options(file: &mut File, db_option: &DbOption) {
        // write database header
        let mut header_buf = vec![0u8; DB_HEADER_SIZE];
        let page_size = db_option.page_size as u32;
        BigEndian::write_u32(
            header_buf.index_mut(RangeFrom {
                start: DB_PAGE_SIZE_OFFSET,
            }),
            page_size,
        );
        file.write_all(header_buf.as_mut_slice()).unwrap();
    }

    fn read_db_options(file: &mut File, db_option: &mut DbOption) {
        // read db options from file and override given options
        let mut header_buf = vec![0u8; DB_HEADER_SIZE];
        file.read(header_buf.as_mut_slice()).unwrap();
        let page_size = BigEndian::read_u32(header_buf.index(RangeFrom {
            start: DB_PAGE_SIZE_OFFSET,
        })) as usize;
        db_option.page_size = page_size;
    }

    pub fn get_page_size(&self) -> usize {
        self.db_option.page_size
    }

    pub fn next_page_index(&mut self) -> usize {
        let next = self.num_pages;
        self.num_pages += 1;
        next
    }

    fn page_offset_in_file(&self, page_index: usize) -> u64 {
        (page_index * self.get_page_size() + DB_HEADER_SIZE) as u64
    }

    pub fn flush(self: &mut Pager, page_index: usize) {
        let offset = self.page_offset_in_file(page_index);
        if let Some(page) = self.pages.borrow().get(&page_index) {
            let mut file = self.file.borrow_mut();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&page.borrow()).unwrap();
        }
    }

    fn load(&self, page_index: usize) {
        let offset = self.page_offset_in_file(page_index);
        let mut buf = vec![0; self.get_page_size()];
        {
            let mut file = self.file.borrow_mut();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.read(buf.as_mut_slice()).unwrap();
        }
        self.pages
            .borrow_mut()
            .insert(page_index, Rc::new(RefCell::new(buf)));
    }

    pub fn page_for_read(self: &Pager, page_index: usize) -> Rc<RefCell<Page>> {
        if page_index >= self.num_pages {
            panic!("read EOF");
        } else if !self.pages.borrow().contains_key(&page_index) {
            self.load(page_index);
        }
        self.pages.borrow().get(&page_index).unwrap().clone()
    }

    pub fn page_for_write(self: &mut Pager, page_index: usize) -> Rc<RefCell<Page>> {
        if page_index > self.num_pages {
            panic!("skipped write to a page");
        } else if page_index == self.num_pages {
            // need a new page
            let new_page = Rc::new(RefCell::new(Page::new_page(self.get_page_size())));
            self.pages.borrow_mut().insert(page_index, new_page);
            self.num_pages += 1;
        } else if !self.pages.borrow().contains_key(&page_index) {
            // load page from file
            self.load(page_index);
        }
        self.pages.borrow().get(&page_index).unwrap().clone()
    }
}
