use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::cell::{Ref, RefCell, RefMut};
use std::ops::{Index, Range};

pub const PAGE_SIZE: usize = 4096;
pub const MAX_PAGE_PER_TABLE: usize = 100;

pub type Page = Vec<u8>;
pub trait PageTrait {
    fn new_page() -> Page;

    fn move_slice_internally(&mut self, from: usize, to: usize, len: usize);

    fn wrap_slice(&mut self, from: usize, buf: &Vec<u8>);
}

impl PageTrait for Page {
    fn new_page() -> Page {
        vec![0u8; PAGE_SIZE]
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
    pages: RefCell<Vec<Option<Page>>>,
    pub num_pages: usize,
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
        if file_size % (PAGE_SIZE as u64) != 0 {
            panic!("Db file is not a whole number of pages. Corrupt file.");
        }
        let num_pages = (file_size / (PAGE_SIZE as u64)) as usize;
        let pages = vec![None; MAX_PAGE_PER_TABLE];
        Pager {
            file: RefCell::new(file),
            pages: RefCell::new(pages),
            num_pages: num_pages,
        }
    }

    pub fn flush(self: &mut Pager, page_index: usize) {
        let offset = page_index * PAGE_SIZE;
        if let Some(page) = self.pages.borrow()[page_index].as_ref() {
            let mut file = self.file.borrow_mut();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(page.as_ref()).unwrap();
        }
    }

    fn load(&self, page_index: usize) {
        let offset = page_index * PAGE_SIZE;
        let mut buf = vec![0; PAGE_SIZE];
        {
            let mut file = self.file.borrow_mut();
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.read(buf.as_mut_slice()).unwrap();
        }
        self.pages.borrow_mut()[page_index] = Some(buf);
    }

    pub fn page_for_read(self: &Pager, page_index: usize) -> Ref<Page> {
        if page_index >= self.num_pages {
            panic!("read EOF");
        } else if self.pages.borrow()[page_index].is_none() {
            self.load(page_index);
        }
        Ref::map(self.pages.borrow(), |pages| {
            pages[page_index].as_ref().unwrap()
        })
    }

    pub fn page_for_write(self: &mut Pager, page_index: usize) -> RefMut<Page> {
        if page_index > self.num_pages {
            panic!("skipped write to a page");
        } else if page_index == self.num_pages {
            // need a new page
            self.pages.borrow_mut()[page_index] = Some(Page::new_page());
            self.num_pages += 1;
        } else if self.pages.borrow()[page_index].is_none() {
            // load page from file
            self.load(page_index);
        }
        RefMut::map(self.pages.borrow_mut(), |pages| {
            pages[page_index].as_mut().unwrap()
        })
    }
}
