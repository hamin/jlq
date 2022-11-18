use rusqlite::Error;
use std::io::BufRead;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use structopt::StructOpt;
use std::fs;


use rusqlite::Connection as SqliteConnection;
use rusqlite::Result;

use linemux::MuxedLines;

use colored_json;
use colored_json::prelude::*;

#[derive(StructOpt, Debug)]
#[structopt(name = "jlq")]
struct Opt {
    // A flag, true if used in the command line. Note doc comment will
    // be used for the help message of the flag. The name of the
    // argument will be, by default, based on the name of the field.
    /// Activate debug mode
    #[structopt(short, long)]
    _debug: bool,

    // The number of occurrences of the `v/verbose` flag
    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    _verbose: u8,

    /// Run SQLite in-memory mode
    #[structopt(short = "m", long)]
    in_memory_storage: bool,

    /// SQLite json query e.g. "json_line->>'level_name' = 'DEBUG'"
    #[structopt(short, long)]
    query: Option<String>,

    #[structopt(short, long)]
    tail: bool,

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

fn get_sqlite_conn(use_in_memory:bool) -> Result<SqliteConnection, Error> {
    if use_in_memory {
        return SqliteConnection::open_in_memory()
    }
    return SqliteConnection::open("test.db");
}

#[tokio::main]
pub async fn main() -> std::io::Result<()> {
    #[cfg(windows)]
    let _enabled = colored_json::enable_ansi_support();

    let opt = Opt::from_args();
    let query = opt.query;
    // println!("{:#?}", opt);
    // println!("{:#?}", opt.query);

    let conn = get_sqlite_conn(opt.in_memory_storage).unwrap();

    conn.execute_batch(&format!(r#"
        CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY, filename TEXT, json_line JSON);
        DELETE FROM logs;
        "#)
    ).unwrap();

    for f in opt.files {

        if opt.tail {
            let full_path =  fs::canonicalize(f).unwrap();
            let mut lines = MuxedLines::new()?;

            lines.add_file(&full_path).await?;

            while let Ok(Some(line)) = lines.next_line().await {
                // println!("({}) {}", line.source().display(), line.line());
                // println!("{:#}", line.line());

                let _insert = conn.execute_batch(&format!(r#"
                    INSERT INTO logs VALUES(null, "{}", '{}');
                    "#, line.source().display(), line.line().replace("'", "''"))
                );
                if let Some(ref q) = query {
                    let _ = filter_logs_by_query(q.to_string(), &conn);
                }
            }
        } else {
            import_logfile(&f, &conn);
        }
    }

    if let Some(q) = query {
        if opt.tail == false {
            let _ = filter_logs_by_query(q, &conn);
        }
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
        if let Ok(l) = log_line?.json_line.to_colored_json_auto() {
            println!("{:#}", l);
        }
    }
    Ok(())
}

