use bytes::{Bytes, BytesMut};
use core::str;
use memchr::memchr;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use tokio::stream;

//use std::convert::From;
use library::radix_tree::{self, RadixTree, RadixTreeNode, StreamRes};

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
//use tokio::sync::Mutex;
use tokio::sync::Notify;

type RedisResult = Result<Option<(usize, RedisBufSplit)>, RESPError>;

enum Ops {
    Get,
    Set,
    Echo,
}

enum KeyValue {
    Some(Bytes),
    None,
}

#[derive(Debug, Eq, Hash, PartialEq)]
enum Value {
    Simple(String),
    Expiry((String, Duration, Instant)),
    None,
}

#[derive(Debug)]
struct BufSplit(usize, usize);

impl BufSplit {
    fn as_slice(self, buf: &BytesMut) -> &[u8] {
        &buf[self.0..self.1]
    }

    fn as_bytes(self, buf: &Bytes) -> Bytes {
        // println!("inside as_bytes");
        // println!("{:?}", buf);
        buf.slice(self.0..self.1)
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, PartialOrd, Ord)]
pub enum RedisValueRef {
    String(Bytes),
    Error(Bytes),
    Int(i64),
    Array(VecDeque<RedisValueRef>),
    NullArray,
    NullBulkString,
    ErrorMsg(Vec<u8>),
}

//impl AsBytes for RedisValueRef {}

#[derive(Debug)]
enum RedisBufSplit {
    Array(Vec<RedisBufSplit>),
    Int(i64),
    String(BufSplit),
    NullBulkString,
    NullArray,
    Error(BufSplit),
}

impl RedisBufSplit {
    fn redis_value(self, buf: &Bytes) -> RedisValueRef {
        match self {
            RedisBufSplit::Array(arr) => {
                //println!("inside redisbufsplit arr");
                RedisValueRef::Array(arr.into_iter().map(|bfs| bfs.redis_value(buf)).collect())
            }
            RedisBufSplit::String(bfs) => {
                //println!("inside redisbufsplit string");
                RedisValueRef::String(bfs.as_bytes(buf))
            }
            RedisBufSplit::Int(i) => RedisValueRef::Int(i),
            RedisBufSplit::NullBulkString => RedisValueRef::NullBulkString,
            RedisBufSplit::Error(bfs) => RedisValueRef::Error(bfs.as_bytes(buf)),
            RedisBufSplit::NullArray => RedisValueRef::NullArray,
        }
    }
}

#[derive(Debug)]
enum RESPError {
    UnexpectedEnd,
    UnknownStartingByte,
    IOError(std::io::Error),
    IntParseFailure,
    BadBulkStringSize(i64),
    BadArraySize(i64),
}

struct DataStructure {
    rt: Arc<Mutex<HashMap<String, radix_tree::RadixTree<HashMap<String, String>>>>>,
    set: Arc<Mutex<HashSet<String>>>,
    hash_map: Arc<Mutex<HashMap<String, Value>>>,
    last_stream_id_map: Arc<Mutex<HashMap<String, String>>>,
    stack: Arc<Mutex<HashMap<String, Arc<Mutex<VecDeque<Vec<String>>>>>>>,
    list: Arc<Mutex<HashMap<String, VecDeque<String>>>>,
}

fn word(buf: &BytesMut, pos: usize) -> Option<(usize, BufSplit)> {
    if buf.len() <= pos {
        return None;
    }
    // println!(
    //     "Inside word,{:?},pos={:?}",
    //     memchr(b'\r', &buf[pos..]).unwrap() + pos + 2,
    //     pos
    // );
    memchr(b'\r', &buf[pos..]).and_then(|end| {
        //println!("end val:{}", end);
        if end + 1 < buf.len() {
            Some((pos + end + 2, BufSplit(pos, pos + end)))
        } else {
            None
        }
    })
}

fn parse(buf: &BytesMut, pos: usize) -> RedisResult {
    // println!(
    //     "inside parse,pos={},val={:?}",
    //     pos,
    //     &buf[pos].to_ascii_lowercase()
    // );
    if buf.is_empty() {
        return Ok(None);
    }
    match buf[pos] {
        b'+' => simple_string(buf, pos + 1),
        b'-' => error(buf, pos + 1),
        b'$' => bulk_string(buf, pos + 1),
        b':' => resp_int(buf, pos + 1),
        b'*' => array(buf, pos + 1),
        _ => Err(RESPError::UnknownStartingByte),
    }
}

fn simple_string(buf: &BytesMut, pos: usize) -> RedisResult {
    //println!("inside Simple string {:?}", pos);
    match word(buf, pos) {
        Some((pos, word)) => Ok(Some((pos, RedisBufSplit::String(word)))),
        None => Ok(None),
    }
}

fn error(buf: &BytesMut, pos: usize) -> RedisResult {
    //println!("inside error {:?}", pos);
    Ok(word(buf, pos).map(|(pos, word)| (pos, RedisBufSplit::Error(word))))
}

fn int(buf: &BytesMut, pos: usize) -> Result<Option<(usize, i64)>, RESPError> {
    //println!("inside int  {:?}", pos);
    match word(buf, pos) {
        Some((pos, word)) => {
            let s = str::from_utf8(word.as_slice(buf))
                .map_err(|_| RESPError::IntParseFailure)
                .unwrap();
            let i = s.parse().map_err(|_| RESPError::IntParseFailure).unwrap();
            Ok(Some((pos, i)))
        }
        None => {
            println!("None in int");
            Ok(None)
        }
    }
}

fn resp_int(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(int(buf, pos)
        .unwrap()
        .map(|(pos, int)| (pos, RedisBufSplit::Int(int))))
}

fn bulk_string(buf: &BytesMut, pos: usize) -> RedisResult {
    //println!("inside bulk string {:?}", pos);
    match int(buf, pos).unwrap() {
        Some((pos, -1)) => Ok(Some((pos, RedisBufSplit::NullBulkString))),
        Some((pos, size)) if size >= 0 => {
            let total_size = pos + size as usize;
            if total_size + 2 > buf.len() {
                println!("None in bulk_string");
                Ok(None)
            } else {
                Ok(Some((
                    total_size + 2,
                    RedisBufSplit::String(BufSplit(pos, total_size)),
                )))
            }
        }

        Some((_pos, bad_size)) => Err(RESPError::BadBulkStringSize(bad_size)),
        None => Ok(None),
    }
}

fn array(buf: &BytesMut, pos: usize) -> RedisResult {
    //println!("inside array {:?}", pos);
    match (int(buf, pos)).unwrap() {
        None => {
            //println!("None in array");
            Ok(None)
        }
        Some((pos, -1)) => Ok(Some((pos, RedisBufSplit::NullArray))),
        Some((pos, num_element)) if num_element >= 0 => {
            let mut res = Vec::with_capacity(num_element as usize);
            let mut curr = pos;
            for _ in 0..num_element {
                //println!("element number : {:?}", i);
                match parse(buf, curr).unwrap() {
                    Some((new_pos, val)) => {
                        res.push(val);
                        curr = new_pos;
                    }
                    None => {
                        println!("hit");
                        return Ok(None);
                    }
                }
                //println!("{:?}", res);
            }
            //println!("hello");
            Ok(Some((curr, RedisBufSplit::Array(res))))
        }
        Some((_pos, bad_num_elements)) => Err(RESPError::BadArraySize(bad_num_elements)),
    }
}

fn encode(item: &RedisValueRef, dst: &mut BytesMut) {
    match item {
        RedisValueRef::Array(arr) => {
            dst.extend_from_slice(b"*");
            dst.extend_from_slice(&arr.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            for redix_value in arr {
                encode(redix_value, dst);
            }
        }
        RedisValueRef::String(s) => {
            dst.extend_from_slice(b"$");
            dst.extend_from_slice(&s.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(&s);
        }
        RedisValueRef::Int(i) => {
            dst.extend_from_slice(b":");
            dst.extend_from_slice(i.to_string().as_bytes());
            dst.extend_from_slice(b"/r/n");
        }
        RedisValueRef::Error(err) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(&err);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::ErrorMsg(err) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(&err);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::NullArray => dst.extend_from_slice("$-1\r\n".as_bytes()),

        RedisValueRef::NullBulkString => dst.extend_from_slice("$-1\r\n".as_bytes()),
    }
}

fn generate_id(curr_stream_id: &str, last_stream_id: &str) -> String {
    let mut res = curr_stream_id.into();
    if curr_stream_id == "*" {
        res = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string();
        res.push_str("-0");
        return res;
    }
    let temp_arr1: Vec<_> = curr_stream_id.split("-").to_owned().collect();
    let temp_arr2: Vec<_> = last_stream_id.split("-").collect();
    if temp_arr1[1] == "*" {
        if temp_arr1[0] == temp_arr2[0] {
            let temp_sequence = (temp_arr2[1].parse::<i32>().unwrap() + 1).to_string();
            let mut temp_val = temp_arr1[0].to_owned();
            temp_val.push_str(&("-".to_owned() + &temp_sequence));
            res = temp_val;
        }
    }
    return res;
}

fn verify_stream(stream_id: &str, last_stream_id: &str) -> bool {
    let arr: Vec<&str> = stream_id.split("-").collect();
    let last_arr: Vec<&str> = last_stream_id.split("-").collect();
    println!(
        "inside verify_stream {:?} {:?} {:?} {:?} stream id",
        stream_id, last_stream_id, arr, last_arr
    );
    if last_arr[0].parse::<i64>().unwrap() > arr[0].parse::<i64>().unwrap() {
        return false;
    }
    if last_arr[0].parse::<i64>().unwrap() == arr[0].parse::<i64>().unwrap() {
        if last_arr[1].parse::<i64>().unwrap() >= arr[1].parse::<i64>().unwrap() {
            return false;
        }
    }
    //println!("returned true");
    return true;
}

async fn stream_write(val: RedisValueRef, stream: &mut tokio::net::TcpStream) {
    let dst = &mut BytesMut::new();
    encode(&val, dst);
    stream.write(dst).await.unwrap();
}

fn get(data_structure: Arc<DataStructure>, arr: Vec<String>) -> Option<RedisValueRef> {
    let mut hash_map = data_structure.hash_map.lock().unwrap();
    let key = &arr[1];
    dbg!("{:?}", &hash_map);
    match hash_map.get(key).unwrap_or(&Value::None) {
        Value::Simple(val) => {
            return Some(RedisValueRef::String(Bytes::copy_from_slice(
                val.as_bytes(),
            )));
        }
        Value::Expiry((val, expiry, start)) => {
            if Instant::now() > start.to_owned() + expiry.to_owned() {
                hash_map.remove(&arr[1]);
                return None;
            } else {
                return Some(RedisValueRef::String(Bytes::copy_from_slice(
                    val.as_bytes(),
                )));
            }
        }
        Value::None => {
            return None;
        }
    }
}

fn set(data_structure: Arc<DataStructure>, arr: Vec<String>) -> Option<RedisValueRef> {
    let data_structure = data_structure.clone();
    let mut map = data_structure.hash_map.lock().unwrap();
    let mut arr_iter = arr.into_iter();
    arr_iter.next();
    let (key, val) = (arr_iter.next().unwrap(), arr_iter.next().unwrap());
    match arr_iter.next() {
        Some(ex_or_px) => match ex_or_px.as_str() {
            "PX" => {
                map.insert(
                    key,
                    Value::Expiry((
                        val,
                        Duration::from_millis(arr_iter.next().unwrap().parse::<u64>().unwrap()),
                        Instant::now(),
                    )),
                );
                return Some(RedisValueRef::String(Bytes::from("OK".as_bytes())));
            }
            "EX" => {
                map.insert(
                    key,
                    Value::Expiry((
                        val,
                        Duration::from_secs(arr_iter.next().unwrap().parse::<u64>().unwrap()),
                        Instant::now(),
                    )),
                );
                return Some(RedisValueRef::String(Bytes::from("OK".as_bytes())));
            }
            _ => None,
        },
        None => {
            map.insert(key, Value::Simple(val));
            return Some(RedisValueRef::String(Bytes::from("OK".as_bytes())));
        }
    }
}

fn rpush(
    data_structure: Arc<DataStructure>,
    arr: Vec<String>,
    notifier: Arc<HashMap<String, Notify>>,
) -> Option<RedisValueRef> {
    let mut arr_iter = arr.into_iter();
    arr_iter.next();
    let key = arr_iter.next().unwrap();
    let temp = data_structure.list.clone();
    let mut temp2 = temp.lock().unwrap();
    let list = temp2.entry(key).or_insert(VecDeque::new());
    let notify2 = notifier.clone();
    let notify = notify2.get("list").unwrap();
    loop {
        match arr_iter.next() {
            Some(val) => {
                list.push_back(val);
                notify.notify_waiters();
            }
            None => {
                break;
            }
        }
    }
    return Some(RedisValueRef::Int(list.len() as i64));
}

fn lpush(
    data_structure: Arc<DataStructure>,
    arr: Vec<String>,
    notifier: Arc<HashMap<String, Notify>>,
) -> Option<RedisValueRef> {
    let mut arr_iter = arr.into_iter();
    arr_iter.next();
    let key = arr_iter.next().unwrap();
    let temp = data_structure.list.clone();
    let mut temp2 = temp.lock().unwrap();
    let list = temp2.entry(key).or_insert(VecDeque::new());
    let notify2 = notifier.clone();
    let notify = notify2.get("list").unwrap();
    loop {
        match arr_iter.next() {
            Some(val) => {
                list.push_front(val);
                notify.notify_waiters();
            }
            None => {
                break;
            }
        }
    }
    return Some(RedisValueRef::Int(list.len() as i64));
}

fn lrange(data_structure: Arc<DataStructure>, arr: Vec<String>) -> Option<RedisValueRef> {
    let mut arr_iter = arr.into_iter();
    arr_iter.next();
    let mut vec = VecDeque::new();
    let (key, mut start, mut end) = (
        arr_iter.next().unwrap(),
        arr_iter.next().unwrap().parse::<i16>().unwrap(),
        arr_iter.next().unwrap().parse::<i16>().unwrap(),
    );
    let temp = data_structure.list.clone();
    let mut temp2 = temp.lock().unwrap();
    let list = temp2.get_mut(&key).unwrap();
    let length = list.len() as i16;
    if start < 0 {
        start = length + start + 1;
    }
    if end < 0 {
        end = length + end + 1;
    }
    for i in start..end + 1 {
        println!("{:?}", i);
        vec.push_back(RedisValueRef::String(Bytes::copy_from_slice(
            &list[i as usize].as_bytes(),
        )));
    }
    return Some(RedisValueRef::Array(vec));
}

fn lpop(data_structure: Arc<DataStructure>, arr: Vec<String>) -> Option<RedisValueRef> {
    let key = &arr[1];
    let mut vec = VecDeque::new();
    let ele = arr
        .get(2)
        .unwrap_or(&"1".to_string())
        .parse::<usize>()
        .unwrap();
    let temp = data_structure.list.clone();
    let mut temp2 = temp.lock().unwrap();
    let list = temp2.get_mut(key).unwrap();
    for _ in 0..ele {
        vec.push_front(RedisValueRef::String(Bytes::copy_from_slice(
            list.pop_front().unwrap().as_bytes(),
        )));
    }
    return Some(RedisValueRef::Array(vec));
}

async fn blpop(
    data_structure: Arc<DataStructure>,
    arr: Vec<String>,
    notifier: Arc<HashMap<String, Notify>>,
) -> Option<RedisValueRef> {
    let (key, timeout) = (&arr[1], arr[2].parse::<usize>().unwrap() as u64);
    let notify2 = notifier.clone();
    let notify = notify2.get("list").unwrap();
    let temp = data_structure.list.clone();
    let start = Instant::now();
    {
        let mut temp2 = temp.lock().unwrap();
        temp2.entry(key.clone()).insert_entry(VecDeque::new());
    }

    if timeout.to_owned() == 0 {
        {
            let mut temp2 = temp.lock().unwrap();
            let list = temp2.get_mut(key).unwrap();
            println!("list len {:?}", list.len());
            if list.len() > 0 as usize {
                return Some(RedisValueRef::String(Bytes::copy_from_slice(
                    list.pop_front().unwrap().as_bytes(),
                )));
            }
        }
        notify.notified().await;
        {
            let mut temp2 = temp.lock().unwrap();
            let list = temp2.get_mut(key).unwrap();
            return Some(RedisValueRef::String(Bytes::copy_from_slice(
                list.pop_front().unwrap().as_bytes(),
            )));
        }
    }
    while start.elapsed() < Duration::from_secs(timeout) {
        {
            let mut temp2 = temp.lock().unwrap();
            let mut list = temp2.get_mut(key).unwrap().clone();
            if list.len() > 0 as usize {
                return Some(RedisValueRef::String(Bytes::copy_from_slice(
                    list.pop_front().unwrap().as_bytes(),
                )));
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    return None;
}

fn multi(
    stack: Arc<Mutex<HashMap<String, Arc<Mutex<VecDeque<Vec<String>>>>>>>,
    val: Vec<String>,
    socket_id: &String,
) {
    dbg!("Inside Multi");
    let mut stack1 = stack.lock().unwrap();
    match stack1.get(socket_id) {
        Some(v) => {
            v.lock().unwrap().push_back(val);
        }
        None => {
            stack1.insert(socket_id.to_owned(), Arc::new(Mutex::new(VecDeque::new())));
        }
    }
    // stack1
    //     .entry(socket_id.to_owned())
    //     .or_insert(Arc::new(Mutex::new(VecDeque::new())))
    //     .lock()
    //     .unwrap()
    //     .push_back(val);
}

async fn exec(
    stream: &mut tokio::net::TcpStream,
    data_structure: Arc<DataStructure>,
    notify2: Arc<HashMap<String, Notify>>,
    socket_id: &String,
) {
    dbg!("Inside EXEC");
    let stack1 = data_structure.stack.clone();
    //dbg!("Inside EXEC STACK length", stack.len());
    loop {
        let query;
        {
            let stack2 = stack1.lock().unwrap();
            let mut stack = stack2.get(socket_id).unwrap().lock().unwrap();

            if stack.len() == 0 {
                return;
            } else {
                query = stack.pop_front().unwrap();
            }
        }
        //dbg!("query", &query);
        Box::pin(logic(
            stream,
            query,
            data_structure.clone(),
            notify2.clone(),
            0,
            socket_id,
        ))
        .await;
    }
}

fn incr(arr: Vec<String>, data_structure: Arc<DataStructure>) -> Option<RedisValueRef> {
    let mut set = data_structure.hash_map.lock().unwrap();
    match set.get_mut(&arr[1]) {
        Some(value) => match value {
            Value::Simple(s) => match s.parse::<i64>() {
                Ok(v) => {
                    set.insert(arr[1].to_owned(), Value::Simple((v + 1).to_string()));
                    return Some(RedisValueRef::Int(v + 1));
                }
                Err(err) => return Some(RedisValueRef::Error(Bytes::from("1"))), //todo!
            },
            Value::Expiry((s, d, i)) => {
                match s.parse::<i64>() {
                    Ok(v) => {
                        let (nd, ni) = (d.clone(), i.clone());
                        set.insert(
                            arr[1].to_owned(),
                            Value::Expiry(((v + 1).to_string(), nd, ni)),
                        );
                        return Some(RedisValueRef::Int(v + 1));
                    }
                    Err(err) => return Some(RedisValueRef::Error(Bytes::from("1"))), //todo!
                }
            }
            Value::None => return Some(RedisValueRef::Error(Bytes::from("1"))),
        },
        None => {
            set.insert(arr[1].to_owned(), Value::Simple("1".to_string()));
            return Some(RedisValueRef::Int(1));
        }
    }
}
fn xadd(
    arr: Vec<String>,
    rt: Arc<Mutex<HashMap<String, RadixTree<HashMap<String, String>>>>>,
    last_stream_id_map: Arc<Mutex<HashMap<String, String>>>,
    notify2: Arc<HashMap<String, Notify>>,
) -> Option<RedisValueRef> {
    let key = &arr[1];
    let mut stream_id = arr[2].clone();
    let mut temp1 = rt.lock().unwrap();
    let rt = temp1
        .entry(key.to_string())
        .or_insert(radix_tree::RadixTree::new(RadixTreeNode {
            children: HashMap::new(),
            value: Some(HashMap::<String, String>::new()),
            end: false,
        }));
    let notify2 = notify2.get("rt").unwrap();

    let mut last_stream_id_map = last_stream_id_map.lock().unwrap();
    let last_stream_id = last_stream_id_map
        .entry(key.to_string())
        .or_insert("0-0".to_string());

    stream_id = generate_id(&stream_id, last_stream_id);
    if !verify_stream(&stream_id, &last_stream_id) {
        println!("last stream id is greater than curr");
        return None;
    }

    let mut val: HashMap<String, String> = HashMap::new();

    for i in (3..arr.len()).step_by(2) {
        val.insert(arr[i].clone(), arr[i + 1].clone());
    }

    if stream_id > last_stream_id_map[key] {
        last_stream_id_map.insert(key.to_string(), stream_id.clone());
    }
    rt.insert(stream_id.clone(), val);
    notify2.notify_waiters();
    println!(
        "radix tree from main HIIII {:#?} also last_stream_id_Map {:?}",
        rt, last_stream_id_map
    );
    return Some(RedisValueRef::String(Bytes::copy_from_slice(
        stream_id.as_bytes(),
    )));
}

fn xrange(
    arr: Vec<String>,
    rt: Arc<Mutex<HashMap<String, RadixTree<HashMap<String, String>>>>>,
) -> Option<RedisValueRef> {
    let key = &arr[1];
    let temp1 = rt.lock().unwrap();
    let rt = temp1.get(key).unwrap();
    let (mut start, mut end) = (arr[2].clone(), arr[3].clone());
    let (k1, k2) = ("0-0".to_string(), "999999999999-9".to_string());
    if start == "-" {
        start = k1;
    }
    if end == "+" {
        end = k2;
    }
    if !start.contains("-") {
        start.push_str("-0");
    }
    if !end.contains("-") {
        end.push_str("-0");
    }
    let mut buffer: Vec<_> = Vec::new();
    radix_tree::RadixTree::search_in_range(
        &rt.root,
        &start,
        &end,
        "".to_string(),
        &mut buffer,
        start.len(),
        end.len(),
    );
    println!("inside xread arr={:?}", buffer);
    return Some(vec_to_redis_value_ref(buffer));
}

fn vec_to_redis_value_ref(buffer: Vec<StreamRes<String>>) -> RedisValueRef {
    let mut res = VecDeque::new();
    for v in buffer {
        match v {
            StreamRes::One(a) => {
                res.push_back(RedisValueRef::Array(VecDeque::from([
                    RedisValueRef::String(Bytes::from_owner(a.into_bytes())),
                ])));
            }
            StreamRes::Two((a, b)) => {
                if let RedisValueRef::Array(vec) = res.get_mut(res.len() - 1).unwrap() {
                    vec.push_back(RedisValueRef::Array(VecDeque::from([
                        RedisValueRef::String(Bytes::from(a.into_bytes())),
                        RedisValueRef::String(Bytes::from(b.into_bytes())),
                    ])));
                }
            }
        }
    }
    println!("inside vec_to_redis_value_ref {:?}", res);
    return RedisValueRef::Array(res);
}

async fn xread(
    arr: Vec<String>,
    rt: Arc<Mutex<HashMap<String, RadixTree<HashMap<String, String>>>>>,
    last_stream_id_map: Arc<Mutex<HashMap<String, String>>>,
    notify2: Arc<HashMap<String, Notify>>,
) -> Option<RedisValueRef> {
    println!("Inside XREAD");
    //let temp1 = rt.lock().unwrap();
    //let mut res = Vec::new();
    let mut res = Vec::new();

    if arr[1] == "BLOCK" {
        println!("inside block of xread");
        let key = &arr[4];
        let given_stream_id;
        let notify2 = notify2.get("rt").unwrap();
        //let rt = rt.lock().unwrap().get(key).unwrap();
        {
            let last_stream_id = last_stream_id_map.lock().unwrap();
            if arr[5] == "$" {
                let mut splitted: Vec<String> = last_stream_id[key]
                    .clone()
                    .split("-")
                    .map(|a| a.to_string())
                    .collect();
                // given_stream_id = .1.parse;
                let k = (splitted[1]
                    .parse::<usize>()
                    .expect("unable to parse it as usize")
                    + 1)
                .to_string();
                splitted[0].push_str(k.as_str());
                given_stream_id = splitted[0].clone();
            } else {
                given_stream_id = arr[5].clone();
            }
        }

        let time = Duration::new(arr[2].parse::<u64>().unwrap(), 0);
        let start_time = SystemTime::now();

        if time == Duration::new(0, 0) {
            println!("inside duration of 0 for xread block");
            {
                let last_stream_id = last_stream_id_map.lock().unwrap().get(key).unwrap().clone();
                if last_stream_id <= given_stream_id {
                    println!(
                        "inside yield_now() of duration of 0 for xread block, last_stream_id{:?} and given_stream_id:{:?}",
                        last_stream_id, given_stream_id
                    );
                    //yield_now();
                    notify2.notified().await;
                }
                {
                    let temp1 = rt.lock().unwrap();
                    let rt = temp1.get(key).unwrap();
                    let end = "99999999999-9";
                    radix_tree::RadixTree::search_in_range(
                        &rt.root,
                        &given_stream_id,
                        &end,
                        "".to_string(),
                        &mut res,
                        given_stream_id.len(),
                        end.len(),
                    );
                }
            }
        } else {
            while SystemTime::now().duration_since(start_time).unwrap() < time {
                {
                    {
                        let last_stream_id = last_stream_id_map.lock().unwrap();
                        {
                            //println!("inside duration of non zero for xread block ");
                            if last_stream_id[key] >= given_stream_id {
                                {
                                    let temp1 = rt.lock().unwrap();
                                    let rt = temp1.get(key).unwrap();
                                    let end = "99999999999-9";
                                    radix_tree::RadixTree::search_in_range(
                                        &rt.root,
                                        &given_stream_id,
                                        &end,
                                        "".to_string(),
                                        &mut res,
                                        given_stream_id.len(),
                                        end.len(),
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    tokio::time::sleep(Duration::new(0, 100)).await;
                }
            }
        }
    } else {
        println!("inside non block section of xread");
        let length = arr.len();
        for i in 2..length / 2 + 1 {
            {
                let temp = rt.lock().unwrap();
                let rt = temp.get(&arr[i]).unwrap();
                let end = "99999999999-9";
                RadixTree::search_in_range(
                    &rt.root,
                    &arr[i + (length - 2) / 2],
                    &end,
                    "".to_string(),
                    &mut res,
                    arr[i + (length - 2) / 2].len(),
                    end.len(),
                );
            }
        }
    }
    if res.len() > 0 {
        return Some(vec_to_redis_value_ref(res));
    } else {
        return Some(RedisValueRef::NullArray);
    }
}

fn discard(data_structure: Arc<DataStructure>, socket_id: &String) {
    let mut stack1 = data_structure.stack.lock().unwrap();
    let mut stack = stack1.get_mut(socket_id).unwrap().lock().unwrap();
    //let mut stack = stack1.lock().unwrap();
    println!("stack {:?}", stack);
    stack.clear();
}

async fn process(
    stream1: Option<tokio::net::TcpStream>,
    data_structure: Option<Arc<DataStructure>>,
    notifier: Option<Arc<HashMap<String, Notify>>>,
    socket_id: String,
) {
    dbg!("inside process function");
    let mut buf = BytesMut::zeroed(512);
    println!("outer loop");
    println!("inside tokyo");
    let (mut stream, data_structure, notifier) = (
        stream1.unwrap(),
        data_structure.unwrap(),
        notifier.unwrap().clone(),
    );

    let mut state = 0;
    loop {
        let length: usize = stream.read(&mut buf).await.unwrap();
        println!(" same client buffer {:?}", buf);

        if length == 0 {
            //println!("no new info");
            continue;
        }

        let mut splited = String::from_utf8(buf[..length].to_vec()).unwrap();
        splited.pop();
        let arr1 = splited.split(" ");
        println!("{:?} arr1", arr1);
        let mut arr = Vec::new();
        for s in arr1 {
            println!("{:?} s", s);
            arr.push(s.to_string());
        }
        println!("{:?} arr for sanity", arr);

        if arr[0] == "MULTI" {
            state = 1;
        }
        if arr[0] == "EXEC" {
            state = 0;
        }

        logic(
            &mut stream,
            arr,
            data_structure.clone(),
            notifier.clone(),
            state,
            &socket_id,
        )
        .await;
        //buf.clear();
    }
}

async fn logic(
    stream: &mut tokio::net::TcpStream,
    arr: Vec<String>,
    data_structure: Arc<DataStructure>,
    notifier: Arc<HashMap<String, Notify>>,
    state: u32,
    socket_id: &String,
) {
    let (rt, _hash_set, _hash_map, last_stream_id_map, stack) = (
        data_structure.rt.clone(),
        data_structure.set.clone(),
        data_structure.hash_map.clone(),
        data_structure.last_stream_id_map.clone(),
        data_structure.stack.clone(),
    );
    dbg!("inside logic function,state arr", state, &arr);
    if state == 1 {
        dbg!("stack", &stack);
        if arr[0].as_str() == "DISCARD" {
            discard(data_structure, socket_id);
        } else {
            multi(stack, arr, socket_id);
        }
        return;
    }
    match arr[0].as_str() {
        "ECHO" => {
            println!("inside echo");
            stream.write(&arr[1].as_bytes()).await.unwrap();
        }

        "GET" => {
            let temp = get(data_structure, arr);
            if let Some(val) = temp {
                stream_write(val, stream).await;
            }
        }

        "SET" => {
            let temp = set(data_structure, arr);
            if let Some(val) = temp {
                stream_write(val, stream).await;
            }
        }

        "RPUSH" => {
            let temp = rpush(data_structure, arr, notifier);
            if let Some(val) = temp {
                stream_write(val, stream).await;
            }
        }

        "LPUSH" => {
            let temp = lpush(data_structure, arr, notifier);
            if let Some(val) = temp {
                stream_write(val, stream).await;
            }
        }

        "LLEN" => {
            let key = &arr[1];
            let temp = data_structure.list.clone();
            let mut temp2 = temp.lock().unwrap();
            let list = temp2.get_mut(key).unwrap();
            list.len();
        }

        "LRANGE" => {
            let temp = lrange(data_structure, arr);
            if let Some(val) = temp {
                stream_write(val, stream).await;
            }
        }

        "LPOP" => {
            let temp = lpop(data_structure, arr);
            if let Some(val) = temp {
                stream_write(val, stream).await;
            }
        }

        "BLPOP" => {
            let temp = blpop(data_structure, arr, notifier).await;
            if let Some(val) = temp {
                stream_write(val, stream).await;
            }
        }

        "XADD" => {
            let temp = xadd(
                arr,
                rt.clone(),
                last_stream_id_map.clone(),
                notifier.clone(),
            );
            if let Some(val) = temp {
                stream_write(val, stream).await;
            }
        }
        "XRANGE" => {
            let temp = xrange(arr, rt.clone());
            if let Some(val) = temp {
                stream_write(val, stream).await;
            }
        }

        "XREAD" => {
            let temp = xread(
                arr,
                rt.clone(),
                last_stream_id_map.clone(),
                notifier.clone(),
            )
            .await;
            if let Some(val) = temp {
                stream_write(val, stream).await;
            }
        }
        "INCR" => {
            let temp = incr(arr, data_structure);
            if let Some(val) = temp {
                stream_write(val, stream).await;
            }
        }

        "MULTI" => {
            multi(stack.clone(), arr, socket_id);
        }

        "EXEC" => {
            exec(stream, data_structure, notifier, socket_id).await;
        }
        "DISCARD" => {
            discard(data_structure, socket_id);
        }
        _ => {}
    }
}

#[tokio::main]
async fn main() {
    println!("{:?}", Bytes::from("Hello, world!"));

    let listener = TcpListener::bind("127.0.0.1:9092").await.unwrap();
    let notifier = Arc::new(HashMap::from([
        ("rt".to_owned(), Notify::new()),
        ("list".to_owned(), Notify::new()),
    ]));
    let data_structure = Arc::new(DataStructure {
        rt: Arc::new(Mutex::new(HashMap::new())),
        set: Arc::new(Mutex::new(HashSet::new())),
        hash_map: Arc::new(Mutex::new(HashMap::new())),
        last_stream_id_map: Arc::new(Mutex::new(HashMap::new())),
        stack: Arc::new(Mutex::new(HashMap::new())),
        list: Arc::new(Mutex::new(HashMap::new())),
    });

    loop {
        let (stream, socket_id) = listener.accept().await.unwrap();
        let notifier = notifier.clone();
        let data_structure = data_structure.clone();

        tokio::spawn(async move {
            //let mut buf = [0; 512];
            process(
                Some(stream),
                Some(data_structure),
                Some(notifier),
                socket_id.to_string(),
            )
            .await;
        });
    }
}
