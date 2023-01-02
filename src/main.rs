use rusqlite::Error;
use rusqlite::params;

use std::io::BufRead;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use structopt::StructOpt;
use std::fs;

use rusqlite::Connection as SqliteConnection;
use rusqlite::Result;

use linemux::MuxedLines;

// use colored_json;
use colored_json::prelude::*;
// use indicatif::ProgressBar; // TODO: some progressbar cleanup

#[derive(StructOpt, Debug)]
#[doc = include_str!("../about.md")]
#[structopt(name = "jlq")]
struct Opt {
    // A flag, true if used in the command line. Note doc comment will
    // be used for the help message of the flag. The name of the
    // argument will be, by default, based on the name of the field.
    /// Activate debug mode
    #[structopt(short, long)]
    debug: bool,

    // The number of occurrences of the `v/verbose` flag
    /// Verbose mode (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: u8,

    /// Run SQLite in-memory mode
    #[structopt(short = "m", long)]
    in_memory_storage: bool,

    /// SQLite json query e.g. "log_line->>'level_name' = 'DEBUG'"
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
    _id: i64,
    _filename: String,
    log_line: String,
}

fn get_sqlite_conn(use_in_memory:bool) -> Result<SqliteConnection, Error> {
    if use_in_memory {
        return SqliteConnection::open_in_memory()
    }
    SqliteConnection::open("test.db")
}

#[warn(clippy::pedantic)]
#[tokio::main]
pub async fn main() -> std::io::Result<()> {
    #[cfg(windows)]
    let _enabled = colored_json::enable_ansi_support();

    let opt = Opt::from_args();
    let query = &opt.query;

    if opt.debug {
        println!("*** Options: {:#?} ***", opt);
        println!("*** Query {:#?} ***", query);
    }

    let conn = get_sqlite_conn(opt.in_memory_storage).expect("Unable to get SQlite connection!");

    conn.execute_batch(r#"
        CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY, filename TEXT, log_line JSON);
        DELETE FROM logs;
        "#
    ).expect("Unable to create 'logs' Table!");

    if opt.tail {
        let mut lines = MuxedLines::new()?;

        for f in opt.files {
            let filename = f.file_name().expect("Missing logfile!").to_str().expect("Could not convert filename to string!");
            let missing_filename_msg = format!("Logfile not found: {:#}", filename);
            let full_path =  fs::canonicalize(f).expect(&missing_filename_msg);
            lines.add_file(&full_path).await?;
        }

        // When tailing, we're going to use WAL so SQLite file can externally be queried too if wanted/needed. Also yields better performance.
        conn.pragma_update(None, "synchronous", "normal").unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();

        let mut stmt = conn.prepare_cached("INSERT INTO logs (filename, log_line) VALUES(?1, ?2)");

        while let Ok(Some(line)) = lines.next_line().await {
            if opt.verbose > 1 {
                println!("*** Tailed New Line: ({}) {} ***", line.source().display(), line.line());
            }

            let log_file = line.source().to_str().expect("No Fillename for Tailed File!");
            let log_line = line.line().replace('\'', "''");


            let _insert = stmt.as_mut().expect("Import Prepare Statement Failed!").execute(params![log_file, log_line]);
            if let Some(ref q) = query {
                let _f = filter_log_line_by_query(log_line, q.to_string(), &conn);
            }
        }
    } else {
        for f in opt.files {
            import_logfile(&f, &conn, opt.debug);
        }
        // let _ = query.as_ref().and_then(|q| Some(filter_logs_by_query(q.to_string(), &conn)));
        if let Some(q) = query {
            let _f = filter_logs_by_query(q.to_string(), &conn);
        }
    }

    Ok(())
}

fn import_logfile(pb:&PathBuf, conn:&rusqlite::Connection, debug:bool) {
    let full_path =  fs::canonicalize(pb).unwrap();
    let filename = full_path.display();

    if debug {
        println!("*** Import Full Filepath: {} ***", filename);
    }

    let f = File::open(&full_path).unwrap();

    // For Bulk Importing a logfile, setting these pragmas yields the best performance.
    let _ = conn.pragma_update(None, "journal_mode", "OFF");
    let _ = conn.pragma_update(None, "synchronous", "0");
    let _ = conn.pragma_update(None, "cache_size", "1000000");
    let _ = conn.pragma_update(None, "locking_mode", "EXCLUSIVE");
    let _ = conn.pragma_update(None, "temp_store", "MEMORY");

    let reader = BufReader::new(f);
    // TODO: some progressbar cleanup
    // let bar = ProgressBar::new(8185995);
    let mut stmt = conn.prepare_cached("INSERT INTO logs (filename, log_line) VALUES(?1, ?2)");
    let tx = conn.unchecked_transaction();
    for line in reader.lines() {
        match line {
            Ok(l) => {
                let insert = stmt.as_mut().expect("Import Prepare Statement Failed!").execute(params![ format!("{}",filename), l.replace('\'', "''")]);

                match insert {
                    Ok(_) => {
                        if debug {
                            println!("*** Importing {:#} ***", l.replace('\'', "''"));
                        }
                    }
                    Err(err) => {
                        panic!("Failed to insert! {:#}", err);
                    }
                }
            }
            Err(err) => {
                panic!("Error reading lines: {:#}", err);
            }
        }
        // TODO: some progressbar cleanup
        // bar.inc(1);
    }
    let _ = tx.expect("Import Transaction Failed!").commit();

    // TODO: some progressbar cleanup
    // bar.finish();
}

fn filter_logs_by_query(query: String, conn:&rusqlite::Connection) -> Result<(), rusqlite::Error> {
    let q = format!(r#"
        SELECT * FROM logs WHERE json_valid(log_line) AND {};
        "#, query);

    let mut stmt = conn.prepare(&q)?;
    let log_iter = stmt.query_map([], |row| {
        Ok(Log {
            _id: row.get(0)?,
            _filename: row.get(1)?,
            log_line: row.get(2)?,
        })
    })?;

    for log_line in log_iter {
        if let Ok(l) = log_line?.log_line.to_colored_json_auto() {
            println!("{:#}", l);
        }
    }
    Ok(())
}

fn filter_log_line_by_query(line: String, query: String, conn:&rusqlite::Connection) -> Result<(), rusqlite::Error> {
    let query_with_json_log = query.replace("log_line", &format!("'{:#}'", line));
    let q = format!(r#"
        SELECT json_valid('{}') AND {};
        "#, line, query_with_json_log);

    let res : i64 = conn.query_row(&q, [], |r| r.get(0)).unwrap();

    if res == 1 {
        if let Ok(l) = line.to_colored_json_auto() {
            println!("{:#}", l);
        }
    }
    Ok(())
}

