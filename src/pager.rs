use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write};
use std::ops::{RangeFrom, Index, IndexMut};

use byteorder::{BigEndian, ByteOrder};


const PAGE_SIZE: usize = 4096;
const MAX_PAGE_PER_TABLE: usize = 100;
pub const ROW_SIZE: usize = 4 + 32 + 256;

const NODE_TYPE_SIZE: usize = 1;
const IS_ROOT_SIZE: usize = 1;
const PARENT_POINTER_SIZE: usize = 4;
const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

const NUM_CELLS_OFFSET: usize = 2;
const NUM_CELLS_SIZE: usize = 4;
const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + NUM_CELLS_SIZE;

const CELL_OFFSET: usize = LEAF_NODE_HEADER_SIZE;
pub const CELL_KEY_SIZE: usize = 4;
const CELL_VALUE_SIZE: usize = ROW_SIZE;
const LEAF_NODE_CELL_SIZE: usize = CELL_KEY_SIZE + CELL_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS: usize = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// TODO: find a better place for this method
pub fn print_constants() {
    println!("Constants:");
    println!("ROW_SIZE: {}", ROW_SIZE);
    println!("COMMON_NODE_HEADER_SIZE: {}", COMMON_NODE_HEADER_SIZE);
    println!("LEAF_NODE_HEADER_SIZE: {}", LEAF_NODE_HEADER_SIZE);
    println!("LEAF_NODE_CELL_SIZE: {}", LEAF_NODE_CELL_SIZE);
    println!("LEAF_NODE_SPACE_FOR_CELLS: {}", LEAF_NODE_SPACE_FOR_CELLS);
    println!("LEAF_NODE_MAX_CELLS: {}", LEAF_NODE_MAX_CELLS);
}

pub type Page = Vec<u8>;

pub trait PageTrait {
    fn new() -> Page;

    fn pos_for_cell(cell_index: usize) -> usize;

    fn num_cells(&self) -> u32;

    fn cell_key(&self, cell_index: usize) -> u32;

    fn set_num_cells(&mut self, num_cells: u32);

    fn debug_print(&self);
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
        if cell_index >= self.num_cells() as usize {
            panic!("invalid cell index!");
        }
        let pos = Page::pos_for_cell(cell_index);
        BigEndian::read_u32(self.index(RangeFrom { start: pos }))
    }

    fn set_num_cells(&mut self, num_cells: u32) {
        if (num_cells as usize) > LEAF_NODE_MAX_CELLS {
            panic!("max number of cells exceeded!");
        }
        BigEndian::write_u32(
            self.index_mut(RangeFrom { start: NUM_CELLS_OFFSET }),
            num_cells,
        )
    }

    fn debug_print(&self) {
        let num_cells = self.num_cells();
        println!("leaf (size {})", num_cells);
        for cell_index in 0..(num_cells as usize) {
            println!("  - {} : {}", cell_index, self.cell_key(cell_index));
        }
    }
}

pub struct Pager {
    file: File,
    pages: Vec<Option<Page>>,
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
            file: file,
            pages: pages,
            num_pages: num_pages,
        }
    }

    pub fn flush(self: &mut Pager, page_index: usize) {
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
    pub fn page_for_read(self: &mut Pager, page_index: usize) -> &Page {
        if page_index >= self.num_pages {
            panic!("read EOF");
        } else if let None = self.pages[page_index] {
            self.load(page_index);
        }
        self.pages[page_index].as_ref().unwrap()
    }

    pub fn page_for_write(self: &mut Pager, page_index: usize) -> &mut Page {
        if page_index > self.num_pages {
            panic!("skipped write to a page");
        } else if page_index == self.num_pages {
            // need a new page
            self.pages[page_index] = Some(vec![0; PAGE_SIZE]);
            self.num_pages += 1;
        } else if let None = self.pages[page_index] {
            // load page from file
            self.load(page_index);
        }
        self.pages[page_index].as_mut().unwrap()
    }

    // this method is designed for dev or test purpose only.
    pub fn debug_print(&mut self) {
        println!("Tree:");
        for page_index in 0..self.num_pages {
            let page = self.page_for_read(page_index);
            page.debug_print();
        }
    }
}
