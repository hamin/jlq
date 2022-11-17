use std::io::BufRead;
use std::fs::File;
use std::io::BufReader;


use rusqlite::{Connection as SqliteConnection};

fn main() {
    let filename = "/Users/harisamin/foo.log";
    let conn = SqliteConnection::open("test.db").unwrap();
    conn.execute_batch(&format!(r#"
        CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY, filename TEXT, json_line JSON);
        DELETE FROM logs;
        "#)
    ).unwrap();


    let f = File::open(filename).unwrap();


    let reader = BufReader::new(f);
    for line in reader.lines() {
        match line {
            Ok(l) => {
                let insert = conn.execute_batch(&format!(r#"
                    INSERT INTO logs VALUES(null, "{}", '{}');
                    "#, filename, l.replace("'", "''"))
                );

                match insert {
                    Ok(_) => {
                        println!("{:#}", l.replace("'", "''"));
                    }
                    Err(err) => {
                        println!("*******************************");
                        println!("{:#}", l.replace("'", "''"));
                        panic!("Failed to insert! {:#}", err);
                    }
                }
            }
            Err(_) => {
                panic!("something went wrong")
            }
        }
    }

    conn.close().unwrap();
}

// fn filename() -> Option<()> {
//     let file = "hey.text";
//     let path = Path::new(file);
//     let filename = path.file_name()?.to_str()?;
//     println!("{}",filename);
//     None
// }
