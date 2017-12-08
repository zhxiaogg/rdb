use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::cell::RefCell;
use std::rc::Rc;
use std::ops::{Index, Range};
use std::collections::HashMap;

pub const DEFAULT_PAGE_SIZE: usize = 4096;

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
    page_size: usize,
}

impl Pager {
    pub fn new(file: &str) -> Pager {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)
            .unwrap();
        let file_size = file.metadata().unwrap().len();
        if file_size % (DEFAULT_PAGE_SIZE as u64) != 0 {
            panic!("Db file is not a whole number of pages. Corrupt file.");
        }
        let num_pages = (file_size / (DEFAULT_PAGE_SIZE as u64)) as usize;
        Pager {
            file: RefCell::new(file),
            pages: RefCell::new(HashMap::new()),
            num_pages: num_pages,
            page_size: DEFAULT_PAGE_SIZE,
        }
    }

    pub fn get_page_size(&self) -> usize {
        self.page_size
    }

    pub fn next_page_index(&mut self) -> usize {
        let next = self.num_pages;
        self.num_pages += 1;
        next
    }

    pub fn flush(self: &mut Pager, page_index: usize) {
        let offset = page_index * self.page_size;
        if let Some(page) = self.pages.borrow().get(&page_index) {
            let mut file = self.file.borrow_mut();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(&page.borrow());
        }
    }

    fn load(&self, page_index: usize) {
        let offset = page_index * self.page_size;
        let mut buf = vec![0; self.page_size];
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
            let new_page = Rc::new(RefCell::new(Page::new_page(self.page_size)));
            self.pages.borrow_mut().insert(page_index, new_page);
            self.num_pages += 1;
        } else if !self.pages.borrow().contains_key(&page_index) {
            // load page from file
            self.load(page_index);
        }
        self.pages.borrow().get(&page_index).unwrap().clone()
    }
}
