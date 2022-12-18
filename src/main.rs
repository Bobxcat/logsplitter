use std::{
    collections::{HashMap, VecDeque},
    io::{BufRead, Cursor},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
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
    time,
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
    let info: LineInfo = serde_json::de::from_str(line).unwrap();

    let key = format!(
        "{}_{}_{}",
        info.meta.service,
        info.meta.env,
        info.timestamp.naive_utc().date()
    );

    println!("{key}");

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

        let mut process_line_queue = process_line_queue.lock().await;

        if let Some(line) = process_line_queue.pop_back() {
            //Drop each lock ASAP (before heavy computation)
            drop(process_line_queue);

            //Get the key for each line
            let key = get_key(&line);
            let line = Line {
                target_file: output_key_to_path(key),
                text: line,
            };

            //Queue the line
            let mut output_line_queue = output_line_queue.lock().await;
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

            s.write(&line.target_file, Bytes::from(line.text)).await;
        }
    }

    Ok(())
}

#[derive(Debug)]
struct Line {
    /// Relative path of the target file
    target_file: PathBuf,
    text: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let num_processing_tasks = 4usize;
    let num_output_tasks = 2usize;

    let (task_send, _task_recv) = channel(128);

    let mut line_stream = open_file("example_sets/input1.json.gz").await?;

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

    //Continuously read lines until reaching the end of the file, then end the execution
    loop {
        //Get the next line
        let line = line_stream.next_line().await?;
        if line.is_empty() {
            println!("Last line reached");
            break;
        }
        //Add the line to the processing queue
        {
            let mut lock = process_line_queue.lock().await;
            lock.push_front(line);
        }
    }

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
