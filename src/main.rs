use std::env;

pub mod dal;

fn main() {
    env::set_var("RUST_BACKTRACE","1");
    {
        let mut dal = dal::Dal::new("db.db");
        let mut page = dal.allocate_empty_page();
        page.num = dal.get_next_page();
        page.data = "data".as_bytes().to_vec();

        // commit
        dal.write_page(&page);
        dal.write_free_list();
        // end of context - close file
    }
    println!("------------------");
    let mut dal = dal::Dal::new("db.db");
    let mut page = dal.allocate_empty_page();
    page.num = dal.get_next_page();
    page.data = "data2".as_bytes().to_vec();
    dal.write_page(&page);

    // create a page and release it so the relased pages will be updated
    let page_num = dal.get_next_page();
    dal.release_page(page_num);

    // commit
    dal.write_free_list();
}
