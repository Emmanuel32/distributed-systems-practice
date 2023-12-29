use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{self, BufRead},
    time::Duration,
};

#[derive(Deserialize)]
struct Request<'a> {
    src: &'a str,
    dest: &'a str,
    body: RequestBody<'a>,
}

#[derive(Serialize)]
struct Response<'a> {
    src: &'a str,
    dest: &'a str,
    body: ResponseBody<'a>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum RequestBody<'a> {
    Init {
        msg_id: i64,
        node_id: String,
        node_ids: Vec<String>,
    },
    Topology {
        msg_id: i64,
        topology: HashMap<&'a str, Vec<String>>,
    },
    Poll {
        msg_id: i64,
        offsets: HashMap<&'a str, i64>,
    },
    Send {
        msg_id: i64,
        key: &'a str,
        msg: i64,
    },
    CommitOffsets {
        msg_id: i64,
        offsets: HashMap<&'a str, i64>,
    },
    ListCommittedOffsets {
        msg_id: i64,
        keys: Vec<&'a str>,
    },
    Error,
    GetUpdates {
        msg_id: i64,
        offsets: HashMap<&'a str, i64>,
    },
    GetUpdatesOk {
        in_reply_to: i64,
        updates: HashMap<&'a str, Vec<[i64; 2]>>,
    },
    Sync {
        msg_id: i64,
        offsets: HashMap<&'a str, i64>,
        updates: HashMap<&'a str, Vec<[i64; 2]>>,
    },
    SyncOk {
        in_reply_to: i64,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum ResponseBody<'a> {
    InitOk {
        in_reply_to: i64,
    },
    TopologyOk {
        in_reply_to: i64,
    },
    PollOk {
        in_reply_to: i64,
        msgs: HashMap<&'a str, &'a [[i64; 2]]>,
    },
    SendOk {
        in_reply_to: i64,
        offset: i64,
    },
    CommitOffsetsOk {
        in_reply_to: i64,
    },
    ListCommittedOffsetsOk {
        in_reply_to: i64,
        offsets: HashMap<&'a str, i64>,
    },
    Error {
        in_reply_to: i64,
        code: i8,
        text: &'a str,
    },
    GetUpdates {
        msg_id: i64,
        offsets: &'a HashMap<&'a str, i64>,
    },
    GetUpdatesOk {
        in_reply_to: i64,
        updates: HashMap<&'a str, &'a [[i64; 2]]>,
    },
    Sync {
        msg_id: i64,
        offsets: &'a HashMap<&'a str, i64>,
        updates: &'a HashMap<&'a str, &'a [[i64; 2]]>,
    },
    SyncOk {
        in_reply_to: i64,
    },
}

#[derive(Default)]
struct Node {
    node_id: String,
    node_id_i64: i64,
    node_ids: Vec<String>,
    topology: Vec<String>,
    commited_offsets: HashMap<String, i64>,
    offsets_ready_to_commit: HashMap<String, i64>,
    commited_msgs: HashMap<String, Vec<[i64; 2]>>,
    uncommited_msgs: HashMap<String, Vec<[i64; 2]>>,
    ongoing_syncs: HashMap<i64, usize>,
}

static EMPTY: Vec<[i64; 2]> = Vec::new();

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut node = Node::default();
    // let mut file = std::fs::File::create("/home/eman/gossip-glomers/maelstrom-echo/foo.txt")?;
    for line in stdin.lock().lines() {
        std::thread::sleep(Duration::from_millis(10));
        let line = line?;
        // std::io::Write::write_all(&mut file, line.as_bytes())?;
        let request: Request = serde_json::from_str(&line)?;

        let response_body = match request.body {
            RequestBody::Init {
                msg_id,
                node_id,
                node_ids,
            } => {
                node.node_id_i64 = i64::from_str_radix(&node_id[1..], 10).unwrap();
                node.node_id = node_id;
                node.node_ids = node_ids;
                ResponseBody::InitOk {
                    in_reply_to: msg_id,
                }
            }
            RequestBody::Topology { msg_id, topology } => {
                node.topology = topology.get(node.node_id.as_str()).unwrap().clone();
                ResponseBody::TopologyOk {
                    in_reply_to: msg_id,
                }
            }
            RequestBody::Send { msg_id, key, msg } => {
                // need offset calulation that doesn't collide and checks offsets_ready_to_commit
                let current = node.uncommited_msgs.entry(key.to_owned()).or_default();
                let offset = current.last().map(|x| x[0] + 1).unwrap_or(0);
                current.push([offset, msg]);
                ResponseBody::SendOk {
                    in_reply_to: msg_id,
                    offset,
                }
            }
            RequestBody::Poll { msg_id, offsets } => {
                let msgs = offsets
                    .iter()
                    .map(|(&k, v)| {
                        let logs = node.commited_msgs.get(k).unwrap_or(&EMPTY);
                        (k, &logs[logs.partition_point(|probe| probe[0] < *v)..])
                    })
                    .collect();
                ResponseBody::PollOk {
                    in_reply_to: msg_id,
                    msgs,
                }
            }
            RequestBody::CommitOffsets { msg_id, offsets } => {
                for node_id in &node.node_ids {
                    let response = Response {
                        src: request.dest,
                        dest: &node_id,
                        body: ResponseBody::GetUpdates { msg_id, offsets: &offsets },
                    };
                    println!("{}", serde_json::to_string(&response)?);
                }
                continue;
            }
            RequestBody::ListCommittedOffsets { msg_id, keys } => {
                ResponseBody::ListCommittedOffsetsOk {
                    in_reply_to: msg_id,
                    offsets: keys
                        .iter()
                        .filter_map(|&k| node.commited_offsets.get(k).map(|&v| (k, v)))
                        .collect(),
                }
            }
            RequestBody::Error { .. } => ResponseBody::Error {
                in_reply_to: 0,
                code: 10,
                text: "Boo Not Supported",
            },
            RequestBody::GetUpdates { msg_id, offsets } => {
                let msgs = offsets
                    .iter()
                    .map(|(&k, v)| {
                        let logs = node.uncommited_msgs.get(k).unwrap_or(&EMPTY);
                        (k, &logs[..logs.partition_point(|probe| probe[0] <= *v)])
                    })
                    .collect();
                ResponseBody::PollOk {
                    in_reply_to: msg_id,
                    msgs,
                }
            }
            RequestBody::Sync { msg_id, offsets, updates } => {
                offsets.iter().for_each(|(&k, &v)| {
                    if let Some(logs) = node.uncommited_msgs.get_mut(k) {
                    let partition_point = logs.partition_point(|probe| probe[0] <= v);
                    logs.drain(..partition_point);
                    }
                    node.commited_offsets
                        .entry(k.to_owned())
                        .and_modify(|x| *x = v.max(*x))
                        .or_insert(v);
                });

                updates.iter().for_each(|(&k, v)| {
                    let stuff = node.commited_msgs.entry(k.to_owned()).or_default();
                    stuff.extend_from_slice(&v);
                    stuff.sort_by(|a, b| a[0].cmp(&b[0]));
                });
                
                ResponseBody::SyncOk {
                    in_reply_to: msg_id,
                }
            }
            RequestBody::SyncOk { in_reply_to } => {
                let sync = node.ongoing_syncs.get_mut(&in_reply_to).unwrap();
                *sync -= 1;
                if *sync == 0 {
                    ResponseBody::CommitOffsetsOk { in_reply_to }
                } else {
                    continue;
                }
            },
            RequestBody::GetUpdatesOk { in_reply_to, updates } => {
                for node_id in &node.node_ids {
                    let response = Response {
                        src: request.dest,
                        dest: &node_id,
                        body: ResponseBody::Sync { msg_id: (), offsets: (), updates: () } { msg_id, offsets: &offsets },
                    };
                    println!("{}", serde_json::to_string(&response)?);
                }
                continue;
            }
        };
        let response = Response {
            src: request.dest,
            dest: request.src,
            body: response_body,
        };
        println!("{}", serde_json::to_string(&response)?);
    }
    Ok(())
}
