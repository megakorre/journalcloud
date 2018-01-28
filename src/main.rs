extern crate env_logger;
extern crate systemd;
extern crate serde_json;
extern crate rusoto_core;
extern crate rusoto_logs;
extern crate hyper;

use systemd::journal::{
    JournalRecord,
    Journal,
    JournalFiles,
    JournalSeek
};
use std::{thread, fs};
use std::io::prelude::{Read, Write};
use std::env::var;
use serde_json as json;
use rusoto_core::{DefaultCredentialsProvider, region};
use rusoto_core::{default_tls_client};
use rusoto_logs::{CloudWatchLogsClient, CloudWatchLogs};
use std::time::{SystemTime, UNIX_EPOCH};

fn current_ms() -> i64 {
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_secs() as i64 * 1000 +
        since_the_epoch.subsec_nanos() as i64 / 1_000_000 as i64
}

struct Config {
    aws_region: region::Region,
    cursor_file: String,
    batch_size: u32,
    sleep_time: std::time::Duration,
    log_group_name: String,
    log_stream_name: String,
}

fn read_config() -> Config {
    Config {
        aws_region: region::default_region(),
        batch_size: var("BATCH_SIZE")
            .unwrap_or(String::from("500"))
            .parse().expect("invalid JOURNAL_CURSOR_FILE not a valid u32"),
        cursor_file: var("JOURNAL_CURSOR_FILE")
            .unwrap_or(String::from("/var/lib/cloudjournal/current-cursor")),
        sleep_time: std::time::Duration::from_millis(500),
        log_group_name: var("LOG_GROUP_NAME")
            .expect("Missing LOG_GROUP_NAME envirenoment variable"),
        log_stream_name: var("LOG_STREAM_NAME")
            .expect("Missing LOG_STREAM_NAME envirenoment variable")
    }
}

type Logs = Vec<JournalRecord>;

struct Runtime {
    journal: Journal,
    logs_client: Box<CloudWatchLogs>,
    config: Config,
    aws_cursor: Option<String>
}

#[derive(Debug)]
struct LogBatch {
    cursor: String,
    logs: Logs
}

impl Runtime {
    fn sleep(&self) {
        thread::sleep(self.config.sleep_time);
    }

    fn read_batch(&mut self) -> Option<LogBatch> {
        let mut logs = Vec::new();
        for _i in 0..self.config.batch_size {
            match self.journal.next_record().unwrap() {
                None => break,
                Some(record) => logs.push(record),
            }
        }
        if logs.len() == 0 {
            None
        } else {
            Some(LogBatch {
                logs,
                cursor: self.journal.cursor().unwrap(),
            })
        }
    }

    fn upload_logs(&mut self, batch: &LogBatch) {
        let result = self.logs_client.put_log_events(
            &rusoto_logs::PutLogEventsRequest {
                log_group_name: self.config.log_group_name.clone(),
                log_stream_name: self.config.log_stream_name.clone(),
                sequence_token: self.aws_cursor.clone(),
                log_events: batch.logs.iter().map(
                    |log_line| rusoto_logs::InputLogEvent {
                        message: json::to_string(&log_line).unwrap(),
                        timestamp: current_ms()
                    }
                ).collect()
            }
        ).unwrap();
        self.aws_cursor = result.next_sequence_token;
    }

    fn persist_cursor(&self, batch: &LogBatch) {
        let mut file = fs::File::create(&self.config.cursor_file).unwrap();
        file.write_all(batch.cursor.as_bytes()).unwrap();
    }
}

fn journal_seek_target(file_path: &String) -> JournalSeek {
    match fs::File::open(&file_path) {
        Err(_) => JournalSeek::Head,
        Ok(mut file) => {
            let mut cursor = String::new();
            if 0 == file.read_to_string(&mut cursor).unwrap() {
                JournalSeek::Head
            } else {
                JournalSeek::Cursor { cursor }
            }
        }
    }
}

fn init_journal(config: &Config) -> Journal {
    let mut journal = Journal::open(
        JournalFiles::All, false, true
    ).unwrap();

    journal.seek(
        journal_seek_target(&config.cursor_file)
    ).unwrap();

    return journal;
}

fn init_runtime(config: Config) -> Runtime {
    let journal = init_journal(&config);
    let logs_client = Box::new(CloudWatchLogsClient::new(
        default_tls_client().unwrap(),
        DefaultCredentialsProvider::new().unwrap(),
        config.aws_region.clone(),
    ));
    let aws_cursor = create_or_find_log_stream(&*logs_client, &config);

    Runtime {config, journal, aws_cursor, logs_client}
}

fn create_or_find_log_stream(
    logs_client: &CloudWatchLogs,
    config: &Config
) -> Option<String> {
    let response = logs_client.describe_log_streams(
        &rusoto_logs::DescribeLogStreamsRequest {
            log_group_name: config.log_group_name.clone(),
            log_stream_name_prefix: Some(config.log_stream_name.clone()),
            descending: None,
            limit: None,
            next_token: None,
            order_by: None
        }
    );
    match response.unwrap().log_streams {
        Some(streams) => {
            if streams.len() > 0 {
                return streams[0].upload_sequence_token.clone();
            }
        },
        None => {}
    }
    logs_client.create_log_stream(&rusoto_logs::CreateLogStreamRequest {
        log_group_name: config.log_group_name.clone(),
        log_stream_name: config.log_stream_name.clone(),
    }).unwrap();
    return None;
}

fn main() {
    let _ = env_logger::init();
    let config = read_config();
    let mut runtime = init_runtime(config);
    loop {
        match runtime.read_batch() {
            None => runtime.sleep(),
            Some(batch) => {
                runtime.upload_logs(&batch);
                runtime.persist_cursor(&batch);
            }
        }
    }
}
