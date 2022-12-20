use std::{
    collections::{HashMap, VecDeque},
    io::{BufRead, Cursor, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use chrono::{DateTime, FixedOffset, TimeZone};

use gzip::JsonLinesWriteStreamPool;
use serde::{Deserialize, Deserializer, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{
        broadcast::{channel, Receiver},
        Mutex,
    },
    time::{self, sleep_until},
};

use serde::de::Visitor;

use crate::gzip::open_file;

extern crate tokio;
//
//

//From a file stream, build a stream which gives each json line as a `JsonEntry` object

mod dec;
mod enc;
mod gzip;

pub fn output_key_to_path<P>(key: P) -> PathBuf
where
    P: AsRef<Path>,
{
    let mut p = Path::new("example_sets/out").join(key);
    p.set_extension("json.gz");
    p
}

pub fn get_key(line: &str) -> String {
    let info: LineInfo = serde_json::de::from_str(line)
        .unwrap_or_else(|e| panic!("Error encountered on line:\n    {e}\n    {line}"));
    // let info: LineInfo = serde_json::de::from_str(line).unwrap();

    let key = format!(
        "{}_{}_{}",
        info.meta.service,
        info.meta.env,
        info.timestamp.naive_utc().date()
    );

    key
}

fn parse_timestamp<'de, D>(de: D) -> Result<DateTime<FixedOffset>, D::Error>
where
    D: Deserializer<'de>,
{
    let visitor = DateTimeVisitor;
    de.deserialize_string(visitor)
}

struct DateTimeVisitor;

impl<'de> Visitor<'de> for DateTimeVisitor {
    type Value = DateTime<FixedOffset>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a string in RFC 3339 date-time format")
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        DateTime::parse_from_rfc3339(&v).map_err(|err| E::custom(err))
    }
    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        DateTime::parse_from_rfc3339(v).map_err(|err| E::custom(err))
    }
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        DateTime::parse_from_rfc3339(v).map_err(|err| E::custom(err))
    }
}

#[derive(Deserialize)]
struct LineInfo {
    #[serde(rename = "@timestamp")]
    #[serde(deserialize_with = "parse_timestamp")]
    timestamp: DateTime<FixedOffset>,
    #[serde(rename = "@meta")]
    meta: Meta,
}

#[derive(Serialize, Deserialize)]
struct Meta {
    #[serde(rename = "service")]
    service: String,

    #[serde(rename = "env")]
    env: String,
}

#[derive(Debug, Clone, Copy)]
enum TaskBroadcastMessage {
    Shutdown,
}

async fn process_lines(
    mut recv: Receiver<TaskBroadcastMessage>,
    process_line_queue: Arc<Mutex<VecDeque<String>>>,
    output_line_queue: Arc<Mutex<VecDeque<Line>>>,
) -> tokio::io::Result<()> {
    const LOCAL_QUEUE_SIZE: usize = 5;

    let mut local_process_queue = VecDeque::with_capacity(LOCAL_QUEUE_SIZE);
    let mut local_output_queue = VecDeque::with_capacity(LOCAL_QUEUE_SIZE);
    loop {
        //Handle messages from main thread
        match recv.try_recv() {
            Ok(val) => match val {
                TaskBroadcastMessage::Shutdown => break,
            },
            Err(e) => match e {
                tokio::sync::broadcast::error::TryRecvError::Closed => {
                    println!("Task sender closed before shutdown notification was received")
                }
                tokio::sync::broadcast::error::TryRecvError::Lagged(_num_skipped) => todo!(),
                tokio::sync::broadcast::error::TryRecvError::Empty => (),
            },
        }

        //Process each line

        //Grab the queue
        let mut process_line_queue = process_line_queue.lock().await;

        // if let Some(line_raw) = process_line_queue.pop_back() {
        //     //Drop each lock ASAP (before heavy computation)

        //     drop(process_line_queue);

        //     //Get the key for each line
        //     let key = get_key(&line_raw);
        //     let line = Line {
        //         target_file: output_key_to_path(key),
        //         text: line_raw,
        //     };

        //     //Queue the line
        //     let mut output_line_queue = output_line_queue.lock().await;
        //     output_line_queue.push_front(line);
        // }

        //Populate `local_process_queue`
        for _ in 0..LOCAL_QUEUE_SIZE {
            if let Some(line) = process_line_queue.pop_back() {
                local_process_queue.push_front(line);
            } else {
                break;
            }
        }

        //Drop the global queue handle
        drop(process_line_queue);

        if let Some(line) = local_process_queue.pop_back() {
            //Drop each lock ASAP (before heavy computation)

            //Get the key for each line
            let key = get_key(&line);
            let line = Line {
                target_file: output_key_to_path(key),
                text: line,
            };
            local_output_queue.push_front(line);
        }

        //Queue the line
        let mut output_line_queue = output_line_queue.lock().await;
        while let Some(line) = local_output_queue.pop_back() {
            output_line_queue.push_front(line);
        }
    }

    Ok(())
}

async fn output_lines(
    mut recv: Receiver<TaskBroadcastMessage>,
    output_line_queue: Arc<Mutex<VecDeque<Line>>>,
    output_file_streams: Arc<Mutex<JsonLinesWriteStreamPool>>,
) -> tokio::io::Result<()> {
    loop {
        //Handle messages from main thread
        match recv.try_recv() {
            Ok(val) => match val {
                TaskBroadcastMessage::Shutdown => break,
            },
            Err(e) => match e {
                tokio::sync::broadcast::error::TryRecvError::Closed => {
                    println!("Task sender closed before shutdown notification was received")
                }
                tokio::sync::broadcast::error::TryRecvError::Lagged(_num_skipped) => todo!(),
                tokio::sync::broadcast::error::TryRecvError::Empty => (),
            },
        }
        let mut queue = output_line_queue.lock().await;
        if let Some(line) = queue.pop_back() {
            let mut s = output_file_streams.lock().await;

            s.write_line(line).await;
        } else {
            //Wait for the queue to be filled again
            // tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    Ok(())
}

/// Represents a decoded JSON line
#[derive(Debug)]
pub struct Line {
    /// Relative path of the target file
    pub target_file: PathBuf,
    pub text: String,
}

//TODO:
//Custom thread pools
//  some (4-5) threads deticated to the async runtime which should be mostly IO operations
//  most (the rest) of the threads should be running Gzip decompression/compression
//  so, what this means is the

async fn start() -> anyhow::Result<()> {
    let num_processing_tasks = 10usize;
    let num_output_tasks = 9usize;

    let (task_send, _task_recv) = channel(128);

    let mut line_stream = open_file("example_sets/testdata.json.gz").await?;
    // let mut line_stream = open_file("example_sets/input1.json.gz").await?;

    // The queue of lines to be processed by `process_lines` tasks
    let process_line_queue: Arc<Mutex<VecDeque<String>>> = Arc::new(Mutex::new(VecDeque::new()));
    let mut process_line_handles = Vec::new();

    // The queue of lines to be written to output files
    let output_line_queue: Arc<Mutex<VecDeque<Line>>> = Arc::new(Mutex::new(VecDeque::new()));
    let mut output_line_handles = Vec::new();

    // The pool of live file streams for outputting compressed file data
    let output_file_streams: Arc<Mutex<JsonLinesWriteStreamPool>> =
        Arc::new(Mutex::new(JsonLinesWriteStreamPool::default()));

    //Start the line processing tasks
    //At the moment, these tasks all run on tokio's async pool to avoid using blocking mutex locks
    for _ in 0..num_processing_tasks {
        let p = process_line_queue.clone();
        let o = output_line_queue.clone();
        let r = task_send.subscribe();
        process_line_handles.push(tokio::spawn(process_lines(r, p, o)));
    }

    //Start the output tasks, which take from the output_line_queue and write them to the correct files (with encoding)
    for _ in 0..num_output_tasks {
        let o = output_line_queue.clone();
        let f = output_file_streams.clone();
        let r = task_send.subscribe();
        output_line_handles.push(tokio::spawn(output_lines(r, o, f)));
    }

    let mut num_lines_read = 0;
    const LOCAL_READ_QUEUE_LEN: usize = 10;
    let mut local_read_queue = VecDeque::with_capacity(LOCAL_READ_QUEUE_LEN);
    //Continuously read lines until reaching the end of the file, then end the execution
    loop {
        macro_rules! flush {
            () => {
                //Lock the global queue
                let mut global_queue = process_line_queue.lock().await;
                while let Some(line) = local_read_queue.pop_back() {
                    global_queue.push_front(line);
                }
            };
        }
        //Get the next line
        let line = line_stream.next_line().await?;
        if line.is_empty() {
            //Flush all queued up lines to the global queue before exiting
            flush!();
            println!("Last line reached");
            break;
        }
        num_lines_read += 1;
        local_read_queue.push_front(line);
        //Add the line to the local queue
        //Push the local queue to the global queue if line_read_queue.len() >= LINE_READ_QUEUE_LEN
        if local_read_queue.len() >= LOCAL_READ_QUEUE_LEN {
            flush!();
        }
    }

    println!("Number of lines read: {num_lines_read}");

    //Once execution has ended, wait for all queues to be finished
    //This is not a time-sensitive operation, so checking can occur relatively infrequently
    let mut queue_wait_interval = time::interval(Duration::from_millis(10));
    loop {
        queue_wait_interval.tick().await;
        let p = process_line_queue.lock().await;
        let o = output_line_queue.lock().await;

        if p.len() == 0 && o.len() == 0 {
            break;
        } else {
            // println!("Wating for shutdown\np_queue: {:?}\no_queue: {:?}", p, o)
        }
    }

    //After queues are finished, notify all tasks that shutdown has been initiated and join them back
    task_send.send(TaskBroadcastMessage::Shutdown)?;

    //Join all tasks
    for task in process_line_handles
        .into_iter()
        .chain(output_line_handles.into_iter())
    {
        task.await??;
    }

    // println!("Task shutdown complete. Flushing remaining bytes to output files");
    println!("Task shutdown complete. Exiting now");

    Ok(())
}

#[cfg(feature = "pprof")]
async fn main_pprof() -> anyhow::Result<()> {
    use pprof::protos::{self, Message};
    let pprof_guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .unwrap();

    main_default().await?;

    // Do pprof things
    match pprof_guard.report().build() {
        Ok(report) => {
            println!("Generating and writing a flamegraph from pprof data");
            {
                let file = std::fs::File::create("flamegraph.svg").unwrap();

                let mut options = pprof::flamegraph::Options::default();
                options.image_width = Some(2500);
                report.flamegraph_with_options(file, &mut options).unwrap();
            }
            println!("Generating and writing pprof data to file");
            {
                let mut file = std::fs::File::create("profile.pb").unwrap();
                // pprof::protos::profile::Profile::new();
                let profile = report.pprof().unwrap();

                let mut content = Vec::new();

                profile.write_to_vec(&mut content)?;
                file.write_all(&content)?;

                // println!("report: {}", &report);
            }
        }
        Err(e) => println!("pprof error encountered: {e}"),
    }

    Ok(())
}

async fn main_default() -> anyhow::Result<()> {
    const DISPLAY_TIMINGS: bool = true;
    //Clear the `out` directory since JsonLineWriteStream appends to existing files
    {
        let path = "example_sets/out";
        std::fs::remove_dir_all(path)?;
        std::fs::create_dir(path)?;
    }

    let start_time = SystemTime::now();

    start().await?;

    let run_duration = SystemTime::now().duration_since(start_time)?;
    if DISPLAY_TIMINGS {
        println!("=====");
        println!(
            "Time taken to run program: {:.2}s",
            run_duration.as_secs_f64()
        );
        println!(
            //Print time in ms as well
            "    Time as milliseconds:  {:}ms",
            run_duration.as_millis()
        );
    }

    Ok(())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "pprof")]
    {
        println!("Running with pprof enabled");
        main_pprof().await?;
    }

    if !cfg!(feature = "pprof") {
        println!("Running with pprof disabled");
        main_default().await?;
    }

    Ok(())
}
