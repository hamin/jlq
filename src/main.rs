use std::io::BufRead;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use structopt::StructOpt;
use std::fs;


use rusqlite::Connection as SqliteConnection;
use rusqlite::Result;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    // A flag, true if used in the command line. Note doc comment will
    // be used for the help message of the flag. The name of the
    // argument will be, by default, based on the name of the field.
    /// Activate debug mode
    // #[structopt(short, long)]
    // debug: bool,

    // The number of occurrences of the `v/verbose` flag
    /// Verbose mode (-v, -vv, -vvv, etc.)
    // #[structopt(short, long, parse(from_occurrences))]
    // verbose: u8,

    /// Set speed
    // #[structopt(short, long, default_value = "42")]
    // speed: f64,

    // /// Output file
    // #[structopt(short, long, parse(from_os_str))]
    // output: PathBuf,

    // the long option will be translated by default to kebab case,
    // i.e. `--nb-cars`.
    /// Number of cars
    // #[structopt(short = "c", long)]
    // nb_cars: Option<i32>,

    /// admin_level to consider
    #[structopt(short, long)]
    query: Option<String>,

    /// Files to process
    #[structopt(name = "FILE", parse(from_os_str))]
    files: Vec<PathBuf>,
}

#[derive(Debug)]
struct Log {
    _id: i32,
    _filename: String,
    json_line: String,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    let query = opt.query;
    // println!("{:#?}", opt);
    // println!("{:#?}", opt.query);

    let conn = &SqliteConnection::open("test.db").unwrap();
    conn.execute_batch(&format!(r#"
        CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY, filename TEXT, json_line JSON);
        DELETE FROM logs;
        "#)
    ).unwrap();

    for f in opt.files {
        import_logfile(&f, &conn);
    }

    if let Some(q) = query {
        let _ = filter_logs_by_query(q, conn);
    }

    Ok(())
}

fn import_logfile(pb:&PathBuf, conn:&rusqlite::Connection) {
    let full_path =  fs::canonicalize(pb).unwrap();
    let filename = full_path.display();
    // println!("Full filepath: {}", filename);
    let f = File::open(&full_path).unwrap();

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
                        // println!("{:#}", l.replace("'", "''"));
                    }
                    Err(err) => {
                        println!("*******************************");
                        println!("{:#}", l.replace("'", "''"));
                        panic!("Failed to insert! {:#}", err);
                    }
                }
            }
            Err(err) => {
                panic!("Error reading lines: {:#}", err);
            }
        }
    }
}

fn filter_logs_by_query(query: String, conn:&rusqlite::Connection) -> Result<(), rusqlite::Error> {
    let q = format!(r#"
        SELECT * FROM logs WHERE json_valid(json_line) AND {};
        "#, query);

    let mut stmt = conn.prepare(&q)?;
    let log_iter = stmt.query_map([], |row| {
        Ok(Log {
            _id: row.get(0)?,
            _filename: row.get(1)?,
            json_line: row.get(2)?,
        })
    })?;

    for log_line in log_iter {
        println!("{:#}", log_line?.json_line);
    }
    Ok(())
}

