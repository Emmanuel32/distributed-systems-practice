use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::{self, BufRead}, time::Duration,
};

#[derive(Deserialize, Serialize)]
struct Request<'a> {
    src: &'a str,
    dest: &'a str,
    body: RequestBody<'a>,
}

#[derive(Deserialize, Serialize)]
struct Response<'a> {
    src: &'a str,
    dest: &'a str,
    body: ResponseBody<'a>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum RequestBody<'a> {
    Init {
        msg_id: i64,
        node_id: String,
        node_ids: Vec<String>,
    },
    Echo {
        msg_id: i64,
        echo: &'a str,
    },
    Generate {
        msg_id: i64,
    },
    Broadcast {
        msg_id: i64,
        message: i64,
    },
    Read {
        msg_id: i64,
    },
    ReadOk {
        in_reply_to: i64,
        value: i64,
    },
    Topology {
        msg_id: i64,
        topology: HashMap<&'a str, Vec<String>>,
    },
    Error {
        in_reply_to: i64,
        code: i8,
        text: &'a str,
    },
    Update {
        messages: HashSet<i64>,
    },
    Add {
        msg_id: i64,
        delta: i64,
    },
    CasOk {
        in_reply_to: i64,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum ResponseBody<'a> {
    InitOk {
        in_reply_to: i64,
    },
    EchoOk {
        msg_id: i64,
        in_reply_to: i64,
        echo: &'a str,
    },
    GenerateOk {
        msg_id: i64,
        in_reply_to: i64,
        id: u64,
    },
    BroadcastOk {
        msg_id: i64,
        in_reply_to: i64,
    },
    Read {
        msg_id: i64,
        key: i64,
    },
    ReadOk {
        msg_id: i64,
        in_reply_to: i64,
        value: i64,
    },
    TopologyOk {
        msg_id: i64,
        in_reply_to: i64,
    },
    Error {
        in_reply_to: i64,
        code: i8,
        text: &'a str,
    },
    Update {
        messages: HashSet<i64>,
    },
    AddOk {
        msg_id: i64,
        in_reply_to: i64,
    },
    Cas {
        msg_id: i64,
        key: i64,
        from: i64,
        to: i64,
        create_if_not_exists: bool,
    },
}

#[derive(Default)]
struct Node {
    node_id: String,
    node_ids: Vec<String>,
    topology: Vec<String>,
    uuid_prefix: u32,
    uuid_count: u32,
    messages: HashSet<i64>,
    read_queue: HashMap<i64, String>,
    add_queue: HashMap<i64, (String, i64)>,
    next_msg_id: i64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut node = Node::default();
    // let mut file = std::fs::File::create("/home/eman/gossip-glomers/maelstrom-echo/foo.txt")?;
    for line in stdin.lock().lines() {
        let line = line?;
        // std::io::Write::write_all(&mut file, line.as_bytes())?;
        let request: Request = serde_json::from_str(&line)?;

        let reply = match request.body {
            RequestBody::Init {
                msg_id,
                node_id,
                node_ids,
            } => {
                node.node_id = node_id;
                node.node_ids = node_ids;
                node.uuid_prefix = u32::from_str_radix(&node.node_id[1..], 10)?;
                Response {
                    src: request.dest,
                    dest: request.src,
                    body: ResponseBody::InitOk {
                        in_reply_to: msg_id,
                    },
                }
            }
            RequestBody::Echo { msg_id, echo } => Response {
                src: request.dest,
                dest: request.src,
                body: ResponseBody::EchoOk {
                    msg_id,
                    in_reply_to: msg_id,
                    echo,
                },
            },
            RequestBody::Generate { msg_id } => {
                let id = ((node.uuid_prefix as u64) << 32) + node.uuid_count as u64;
                node.uuid_count += 1;
                Response {
                    src: request.dest,
                    dest: request.src,
                    body: ResponseBody::GenerateOk {
                        msg_id,
                        in_reply_to: msg_id,
                        id,
                    },
                }
            }
            RequestBody::Broadcast { msg_id, message } => {
                node.messages.insert(message);
                for node_id in node.topology.iter() {
                    if node_id != &node.node_id {
                        let msg = Response {
                            src: &node.node_id,
                            dest: &node_id,
                            body: ResponseBody::Update {
                                messages: node.messages.clone(),
                            },
                        };
                        println!("{}", serde_json::to_string(&msg)?);
                    }
                }
                Response {
                    src: request.dest,
                    dest: request.src,
                    body: ResponseBody::BroadcastOk {
                        msg_id,
                        in_reply_to: msg_id,
                    },
                }
            }
            RequestBody::Read { msg_id } => {
                node.read_queue.insert(msg_id, request.src.to_owned());
                std::thread::sleep(Duration::from_millis(60));
                Response {
                    src: &node.node_id,
                    dest: "seq-kv",
                    body: ResponseBody::Read { msg_id, key: 0 },
                }
            }
            RequestBody::Topology { msg_id, topology } => {
                node.topology = topology.get(node.node_id.as_str()).unwrap().clone();
                Response {
                    src: request.dest,
                    dest: request.src,
                    body: ResponseBody::TopologyOk {
                        msg_id,
                        in_reply_to: msg_id,
                    },
                }
            }
            RequestBody::Update { messages } => {
                let len = node.messages.len();
                node.messages.extend(messages);
                if node.messages.len() != len {
                    for node_id in node.topology.iter() {
                        if node_id != &node.node_id && node_id != request.src {
                            let msg = Response {
                                src: &node.node_id,
                                dest: &node_id,
                                body: ResponseBody::Update {
                                    messages: node.messages.clone(),
                                },
                            };
                            println!("{}", serde_json::to_string(&msg)?);
                        }
                    }
                }
                continue;
            }
            RequestBody::Add { msg_id, delta } => {
                node.add_queue
                    .insert(msg_id, (request.src.to_owned(), delta));
                Response {
                    src: &node.node_id,
                    dest: "seq-kv",
                    body: ResponseBody::Read { msg_id, key: 0 },
                }
            }
            RequestBody::ReadOk {
                in_reply_to,
                value,
            } => {
                if let Some(read) = node.read_queue.get(&in_reply_to) {
                    Response {
                        src: &node.node_id,
                        dest: &read,
                        body: ResponseBody::ReadOk {
                            msg_id: in_reply_to,
                            in_reply_to,
                            value,
                        },
                    }
                } else if let Some(add) = node.add_queue.get(&in_reply_to) {
                    Response {
                        src: &node.node_id,
                        dest: "seq-kv",
                        body: ResponseBody::Cas {
                            msg_id: in_reply_to,
                            key: 0,
                            from: value,
                            to: value + add.1,
                            create_if_not_exists: true,
                        },
                    }
                } else {
                    continue;
                }
            }
            RequestBody::CasOk {
                in_reply_to,
            } => {
                if let Some(add) = node.add_queue.get(&in_reply_to) {
                    Response {
                        src: &node.node_id,
                        dest: &add.0,
                        body: ResponseBody::AddOk {
                            msg_id: in_reply_to,
                            in_reply_to,
                        },
                    }
                } else {
                    continue;
                }
            }
            RequestBody::Error {
                in_reply_to,
                code: 20,
                text: _,
            } => {
                if let Some(read) = node.read_queue.get(&in_reply_to) {
                    Response {
                        src: &node.node_id,
                        dest: &read,
                        body: ResponseBody::ReadOk {
                            msg_id: in_reply_to,
                            in_reply_to,
                            value: 0,
                        },
                    }
                } else if let Some(add) = node.add_queue.get(&in_reply_to) {
                    Response {
                        src: &node.node_id,
                        dest: "seq-kv",
                        body: ResponseBody::Cas {
                            msg_id: in_reply_to,
                            key: 0,
                            from: 0,
                            to: add.1,
                            create_if_not_exists: true,
                        },
                    }
                } else {
                    continue;
                }
            }
            RequestBody::Error {
                in_reply_to,
                code: 22,
                text: _,
            } => Response {
                src: &node.node_id,
                dest: "seq-kv",
                body: ResponseBody::Read {
                    msg_id: in_reply_to,
                    key: 0,
                },
            },
            RequestBody::Error {
                in_reply_to: _,
                code: _,
                text: _,
            } => Response {
                src: request.dest,
                dest: request.src,
                body: ResponseBody::Error {
                    in_reply_to: 0,
                    code: 10,
                    text: "Boo Not Supported",
                },
            },
        };
        println!("{}", serde_json::to_string(&reply)?);
    }
    Ok(())
}
