use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    os::unix::prelude::FileExt,
    path::Path,
    usize,
};

extern crate page_size;

const PAGE_NUM_SIZE: usize = 8;
const META_PAGE_NUM: usize = 0;

#[derive(Debug)]
pub struct Dal {
    file: File,
    page_size: usize,
    free_list: FreeList,
    metadata: MetaData,
}

#[derive(Debug)]
pub struct Page {
    pub num: usize,
    pub data: Vec<u8>,
}

impl Dal {
    // if will be better to store path instead file
    pub fn new(path: &str) -> Self {
        let is_file_exists = Path::new(path).exists();
        let file = match is_file_exists {
            true => File::open(path).unwrap(),
            false => File::create(path).unwrap(),
        };

        let mut dal = Dal {
            file,
            page_size: page_size::get(),
            free_list: FreeList::new(0),
            metadata: MetaData::new(),
        };

        if is_file_exists {
            dal.read_metadata();
            dal.read_free_list();
        } else {
            dal.write_metadata();
            dal.write_free_list();
        }

        return dal;
    }

    pub fn allocate_empty_page(&self) -> Page {
        return Page {
            data: Vec::with_capacity(self.page_size),
            // data: vec![0; self.page_size],
            num: 0,
        };
    }

    pub fn get_next_page(&mut self) -> usize {
        let page_num = self.free_list.get_next_page();

        println!("get_next_page: {}", page_num);

        return page_num;
    }

    pub fn release_page(&mut self, page_num: usize) {
        self.free_list.release_page(page_num);
    }

    pub fn read_page(&mut self, page_num: usize) -> Page {
        let mut page = self.allocate_empty_page();

        let offset = page_num * self.page_size;

        println!("offset: {}, page_num: {}", offset, page_num);

        let mut buf = vec![0; self.page_size];
        self.file.seek(SeekFrom::Start(offset as u64)).unwrap();
        self.file.read_exact(&mut buf).unwrap();

        // println!("buf: {:?}", buf);
        page.data = buf;

        return page;
    }

    // done
    pub fn write_page(&self, page: &Page) {
        // write page to file
        let offset = self.page_size * page.num;
        let _ = &self.file.write_at(&page.data, offset as u64);
    }

    // done
    pub fn read_free_list(&mut self) {
        let free_list_page = self.read_page(self.metadata.free_list_page);
        self.free_list.deserialize(free_list_page.data);
    }

    // done
    pub fn write_free_list(&mut self) -> Page {
        let mut page = self.allocate_empty_page();
        page.num = self.metadata.free_list_page;
        self.free_list.serialize(&mut page.data);
        self.write_page(&page);

        return page;
    }

    fn read_metadata(&mut self) {
        let mut metadata_page = self.read_page(META_PAGE_NUM);
        // println!("metadata_page: {:?}", metadata_page);
        self.metadata.deserialize(&mut metadata_page.data);
        // return
    }


    pub fn write_metadata(&mut self) -> Page {
        let mut page = self.allocate_empty_page();
        page.num = META_PAGE_NUM;
        self.metadata.serialize(&mut page.data);
        self.write_page(&page);
        println!("write_metadata: {:#?}", page.data);

        return page;
    }

    // fn close(&mut self) {
    //     // no need for close for file, drops when variable goes out of scope
    //     // self.file.close().unwrap();
    // }
}

#[derive(Debug)]
pub struct FreeList {
    max_page: usize,
    released_pages: Vec<usize>,
}

impl FreeList {
    // initial page
    fn new(max_page: usize) -> Self {
        return FreeList {
            max_page,
            released_pages: Vec::new(),
        };
    }

    fn get_next_page(&mut self) -> usize {
        if !self.released_pages.is_empty() {
            return self.released_pages.pop().unwrap();
        }
        self.max_page += 1;
        return self.max_page;
    }

    fn release_page(&mut self, page_num: usize) {
        self.released_pages.push(page_num);
    }

    fn serialize(&self, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&(self.max_page as u32).to_be_bytes());
        buffer.extend_from_slice(&(self.released_pages.len() as u32).to_be_bytes());

        for page_num in &self.released_pages {
            buffer.extend_from_slice(&page_num.to_be_bytes());
        }
    }

    fn deserialize(&mut self, data: Vec<u8>) {
        let mut position = 0;

        // println!("deserialize: {:?}", data);

        self.max_page = usize::from_be_bytes(data[position..position + 8].try_into().unwrap());
        position += 8;

        let released_pages_count =
            usize::from_be_bytes(data[position..position + 8].try_into().unwrap());
        position += 8;

        for _ in 0..released_pages_count {
            let page_num =
                usize::from_be_bytes(data[position..position + PAGE_NUM_SIZE].try_into().unwrap());
            position += PAGE_NUM_SIZE;

            self.released_pages.push(page_num);
        }
    }
}

#[derive(Debug)]
struct MetaData {
    free_list_page: usize,
}

impl MetaData {
    fn new() -> Self {
        return MetaData {
            free_list_page: META_PAGE_NUM,
        };
    }

    fn serialize(&self, buffer: &mut Vec<u8>) {
        println!("serialize metadata {:?}", self.free_list_page);
        buffer.extend_from_slice(&(self.free_list_page as usize).to_be_bytes());
    }

    fn deserialize(&mut self, buffer: &mut Vec<u8>) {
        println!("deserialize metadata {:?}", buffer[0..8].to_vec());
        if !buffer.is_empty() {
            self.free_list_page = usize::from_be_bytes(buffer[0..8].try_into().unwrap());
            println!("deserialize metadata {:?}", self.free_list_page);
        
        }
    }
}
