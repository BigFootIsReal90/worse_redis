use bytes::{Bytes, BytesMut};
use core::str;
use memchr::memchr;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::net::TcpStream;
//use std::convert::From;
use library::radix_tree::{self, RadixTree, RadixTreeNode};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
//use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::task::yield_now;

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
enum value {
    Simple(RedisValueRef),
    Expiry((RedisValueRef, u128)),
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
    Stream(BTreeMap<RedisValueRef, RedisValueRef>),
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

struct data_structure {
    rt: Arc<Mutex<HashMap<String, radix_tree::RadixTree<HashMap<String, String>>>>>,
    set: Arc<Mutex<HashSet<String>>>,
    hashMap: Arc<Mutex<HashMap<String, String>>>,
    last_stream_id_map: Arc<Mutex<HashMap<String, String>>>,
    stack: Arc<Mutex<Vec<Vec<String>>>>,
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
        Some((pos, bad_num_elements)) => Err(RESPError::BadArraySize(bad_num_elements)),
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
        RedisValueRef::Stream(s) => {
            let temp = s
                .get(&RedisValueRef::String(Bytes::from("stream_key")))
                .unwrap();
            encode(temp, dst)
        }
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
    return true;
}

fn SET(set: Arc<Mutex<HashSet<String>>>, val: String) {
    set.lock().unwrap().insert(val);
}

fn GET(set: &Arc<Mutex<HashSet<String>>>, val: String) -> Option<String> {
    set.lock().unwrap().get(&val).cloned()
}

fn MULTI(stack: Arc<Mutex<Vec<Vec<String>>>>, val: Vec<String>) {
    dbg!("Inside Multi");
    stack.lock().unwrap().push(val);
}

async fn EXEC(
    stream: &mut tokio::net::TcpStream,
    data_structure: Arc<data_structure>,
    notify2: Arc<Notify>,
) {
    dbg!("Inside EXEC");
    let stack1 = data_structure.stack.clone();
    //dbg!("Inside EXEC STACK length", stack.len());
    loop {
        let query;
        {
            let mut stack = stack1.lock().unwrap();
            if stack.len() == 0 {
                return;
            } else {
                query = stack.pop().unwrap();
            }
        }
        //dbg!("query", &query);
        Box::pin(logic(
            stream,
            query,
            data_structure.clone(),
            notify2.clone(),
            0,
        ))
        .await;
    }
}

fn XADD(
    arr: Vec<String>,
    rt: Arc<Mutex<HashMap<String, RadixTree<HashMap<String, String>>>>>,
    last_stream_id_map: Arc<Mutex<HashMap<String, String>>>,
    notify2: Arc<Notify>,
) {
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

    let mut last_stream_id_map = last_stream_id_map.lock().unwrap();
    let last_stream_id = last_stream_id_map
        .entry(key.to_string())
        .or_insert(stream_id.clone());
    if stream_id == "*".to_string() {
        stream_id = generate_id(&stream_id, last_stream_id);
    }
    if !verify_stream(&stream_id, &last_stream_id) {
        println!("last stream id is greater than curr");
        //return false;
    }
    todo!("add support for partial streamid");

    let mut val: HashMap<String, String> = HashMap::new();

    for i in (3..arr.len()).step_by(2) {
        val.insert(arr[i].clone(), arr[i + 1].clone());
    }

    if stream_id > last_stream_id_map[key] {
        last_stream_id_map.insert(key.to_string(), stream_id.clone());
    }
    rt.insert(stream_id, val);
    notify2.notify_one();
    println!(
        "radix tree from main HIIII {:#?} also last_stream_id_Map {:?}",
        rt, last_stream_id_map
    );
}

fn XRANGE(arr: Vec<String>, rt: Arc<Mutex<HashMap<String, RadixTree<HashMap<String, String>>>>>) {
    let key = &arr[1];
    let temp1 = rt.lock().unwrap();
    let rt = temp1.get(key).unwrap();
    let (mut start, mut end) = (&arr[2], &arr[3]);
    let (k1, k2) = ("000000000000-0".to_string(), "999999999999-9".to_string());
    if start == "-" {
        start = &k1;
    }
    if end == "+" {
        end = &k2;
    }

    let mut res: Vec<_> = Vec::new();
    radix_tree::RadixTree::search_in_range(&rt.root, start, end, "".to_string(), &mut res);
    println!("{:?}", res);
}

async fn XREAD(
    arr: Vec<String>,
    rt: Arc<Mutex<HashMap<String, RadixTree<HashMap<String, String>>>>>,
    last_stream_id_map: Arc<Mutex<HashMap<String, String>>>,
    notify2: Arc<Notify>,
) {
    println!("Inside XREAD");
    //let temp1 = rt.lock().unwrap();
    //let mut res = Vec::new();

    if arr[1] == "BLOCK" {
        println!("inside block of xread");
        let (key, blocking_time) = (&arr[4], &arr[2]);
        let given_stream_id;
        //let rt = rt.lock().unwrap().get(key).unwrap();
        {
            let last_stream_id = last_stream_id_map.lock().unwrap();
            if arr[5] == "$" {
                given_stream_id = last_stream_id[key].clone();
            } else {
                given_stream_id = arr[5].clone();
            }
        }
        let time = Duration::new(arr[2].parse::<u64>().unwrap(), 0);
        let start_time = SystemTime::now();
        if time == Duration::new(0, 0) {
            loop {
                println!("inside duration of 0 for xread block");
                {
                    let lastStreamId = last_stream_id_map.lock().unwrap().get(key).unwrap().clone();
                    if lastStreamId <= given_stream_id {
                        println!(
                            "inside yield_now() of duration of 0 for xread block, last_stream_id{:?} and given_stream_id:{:?}",
                            lastStreamId, given_stream_id
                        );
                        //yield_now();
                        notify2.notified().await;
                    } else {
                        println!("else break");
                        break;
                    }
                }
            }
        } else {
            while SystemTime::now().duration_since(start_time).unwrap() < time {
                {
                    let lastStreamId = last_stream_id_map.lock().unwrap();
                    {
                        println!("inside duration of non zero for xread block ");
                        if lastStreamId[key] <= given_stream_id {
                            tokio::time::sleep(Duration::new(0, 100));
                        } else {
                            println!(
                                "FINALLY GOT A VALUE FROM ANOTHER CLIENT WHILE BLOCKED BABYYYY. and it is ",
                                //res
                            );

                            //todo!("write res to stream");
                            break;
                        }
                    }
                }
            }
        }
    } else {
        println!("inside non block section of xread");
        let length = arr.len();
        let mut res = Vec::new();
        for i in 2..length / 2 + 2 {
            {
                let temp = rt.lock().unwrap();
                let rt = temp.get(&arr[i]).unwrap();
                RadixTree::search_in_range(
                    &rt.root,
                    &arr[i / 2 + 2 + i],
                    &"99999999999-9",
                    "".to_string(),
                    &mut res,
                );
            }
        }
    }
}

async fn process(
    mut stream1: Option<tokio::net::TcpStream>,
    data_structure: Option<Arc<data_structure>>,
    notify21: Option<Arc<Notify>>,
) {
    dbg!("inside process function");
    let decoded = Bytes::new();
    let mut buf = BytesMut::zeroed(512);
    println!("outer loop");
    let mut last_stream_id = "000000000-0".to_string();
    println!("inside tokyo");
    let (mut stream, data_structure, notify2) =
        (stream1.unwrap(), data_structure.unwrap(), notify21.unwrap());

    let mut stack = Vec::<String>::new();
    let mut state = 0;
    loop {
        let mut res = BytesMut::zeroed(512);
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
        //println!("buf read babyyy{:?}", splited2);
        // println!("{:?},{:?},{:?}", &buf, &buf[..length], b'*');
        // println!("hola,{:?}", parse(&buf, 0));

        // let temp = parse(&buf, 0)
        //     .expect("error from 2")
        //     .unwrap()
        //     .1
        //     .redis_value(&buf.clone().freeze());
        if arr[0] == "MULTI" {
            state = 1;
            continue;
        }
        if arr[0] == "EXEC" {
            state = 0;
        }

        logic(
            &mut stream,
            arr,
            data_structure.clone(),
            notify2.clone(),
            state,
        )
        .await;
        //buf.clear();
    }
}

async fn logic(
    stream: &mut tokio::net::TcpStream,
    arr: Vec<String>,
    data_structure: Arc<data_structure>,
    notify2: Arc<Notify>,
    state: u32,
) {
    let (rt, hashSet, HashMap, last_stream_id_map, stack) = (
        data_structure.rt.clone(),
        data_structure.set.clone(),
        data_structure.hashMap.clone(),
        data_structure.last_stream_id_map.clone(),
        data_structure.stack.clone(),
    );
    dbg!("inside logic function,state arr", state, &arr);
    if state == 1 {
        dbg!("stack", &stack);
        MULTI(stack, arr);
        return;
    }
    match arr[0].as_str() {
        "ECHO" => {
            println!("inside echo");
            stream.write(&arr[1].as_bytes()).await.unwrap();
        }
        // "GET" => {
        //     let response1 = {
        //         let temp1 = temp2.lock().unwrap();
        //         println!("{:?}", temp1);
        //         match temp1.get(&RedisValueRef::String(Bytes::from(arr[1].clone()))) {
        //             Some(val) => match val {
        //                 value::Simple(val) => {
        //                     encode(val, &mut res);
        //                     KeyValue::Some(res.freeze().clone())
        //                     //stream.write(&res).await.unwrap();
        //                 }
        //                 value::Expiry((val, time)) => {
        //                     {
        //                         if &SystemTime::now()
        //                             .duration_since(UNIX_EPOCH)
        //                             .unwrap()
        //                             .as_millis()
        //                             > time
        //                         {
        //                             temp1.get(&RedisValueRef::String(Bytes::from(
        //                                 arr[1].clone(),
        //                             )));
        //                         } else {
        //                             encode(val, &mut res);
        //                         }
        //                         KeyValue::Some(res.freeze())
        //                     }
        //                     //stream.write(&res).await.unwrap();
        //                 }
        //             },
        //             None => {
        //                 KeyValue::None

        //                 //stream.write(b"$-1\r\n").await.unwrap();
        //             }
        //         }
        //     };
        //     match response1 {
        //         KeyValue::Some(val) => {
        //             stream.write(&val).await.unwrap();
        //         }

        //         _ => {
        //             stream.write(b"$-1\r\n").await.unwrap();
        //         }
        //     };
        // }
        // "SET" => {
        //     println!("inside SET");

        //     {
        //         let mut temp1 = temp2.lock().unwrap();
        //         if arr.len() > 3 {
        //             let time = match &RedisValueRef::String(Bytes::from(arr[4])) {
        //                 RedisValueRef::String(b) => {
        //                     String::from_utf8_lossy(&b).to_string()
        //                 }
        //                 _ => "0".to_string(),
        //             };
        //             let time_required: u128 = SystemTime::now()
        //                 .duration_since(UNIX_EPOCH)
        //                 .unwrap()
        //                 .as_millis()
        //                 + time.parse::<u128>().unwrap();
        //             //let temp = hash_map.lock().unwrap();
        //             temp1.insert(
        //                 RedisValueRef::String(Bytes::from(arr[1])),
        //                 value::Expiry((
        //                     RedisValueRef::String(Bytes::from(arr[2])),
        //                     time_required,
        //                 )),
        //             );
        //         } else {
        //             println!("inside else case");
        //             temp1.insert(
        //                 RedisValueRef::String(Bytes::from(arr[1])),
        //                 value::Simple(RedisValueRef::String(Bytes::from(arr[2]))),
        //             );
        //             //stream.write(b"OK").await.unwrap();
        //         }
        //         //println!("hash_map{:?}", hash_map);
        //     }
        //     stream.write(b"OK").await.unwrap();
        // }
        // "RPUSH" => {
        //     let mut temp1 = temp2.lock().unwrap();
        //     let temp = temp1.get_mut(&arr[1]);
        //     let ans;
        //     let list = match temp {
        //         Some(list) => list,
        //         None => {
        //             temp1.insert(
        //                 arr[1].clone(),
        //                 value::Simple(RedisValueRef::Array(VecDeque::new())),
        //             );
        //             temp1.get_mut(&arr[1]).unwrap()
        //         }
        //     };
        //     if let value::Simple(list1) = list {
        //         if let RedisValueRef::Array(list2) = list1 {
        //             for i in 2..arr.len() {
        //                 list2.push_back(arr[i].clone());
        //             }
        //         }
        //     }
        //     ans = temp1.get(&arr[1].clone()).unwrap();
        //     println!("hellooooooooooo {:?}", ans);
        // }

        // "LPUSH" => {
        //     let mut res = 0;
        //     let mut actual = BytesMut::new();
        //     {
        //         let mut temp1 = temp2.lock().unwrap();
        //         let list = temp1.get_mut(&arr[1]).unwrap();
        //         if let value::Simple(list1) = list {
        //             if let RedisValueRef::Array(list2) = list1 {
        //                 for i in 2..arr.len() {
        //                     list2.push_front(arr[i].clone());
        //                 }
        //                 res = list2.len();
        //             }
        //         }
        //     }
        //     encode(&RedisValueRef::Int(res as i64), &mut actual);
        //     stream.write(&actual).await;
        // }

        // "LLEN" => {
        //     let mut res = BytesMut::new();
        //     let mut ans = 0;
        //     {
        //         let mut temp1 = temp2.lock().unwrap();
        //         let list = temp1.get(&arr[1]);
        //         match list {
        //             Some(val) => match val {
        //                 value::Expiry((list, exp)) => match list {
        //                     RedisValueRef::Array(arr) => ans = arr.len() as i32,
        //                     _ => (),
        //                 },
        //                 value::Simple(list) => match list {
        //                     RedisValueRef::Array(arr) => ans = arr.len() as i32,
        //                     _ => (),
        //                 },
        //             },
        //             None => (),
        //         }
        //     }
        //     encode(&RedisValueRef::Int(ans.into()), &mut res);
        //     stream.write(&res).await;
        // }
        // "LRANGE" => {
        //     let mut res = BytesMut::new();
        //     {
        //         let mut temp1 = temp2.lock().unwrap();
        //         let temp = temp1.get_mut(&arr[1]).unwrap();
        //         let mut temp_list = VecDeque::new();
        //         let mut list = &mut temp_list;
        //         match temp {
        //             value::Simple(val) => match val {
        //                 RedisValueRef::Array(real_list) => {
        //                     list = real_list;
        //                 }
        //                 _ => {}
        //             },
        //             value::Expiry((val, time)) => match val {
        //                 RedisValueRef::Array(real_list) => {
        //                     list = real_list;
        //                 }
        //                 _ => {}
        //             },
        //         }
        //         let mut start;
        //         let mut end;
        //         let total = list.len() as i64;
        //         println!("array two {:?}, total {:?}", arr[2], total);
        //         if let RedisValueRef::Int(i) = arr[2] {
        //             start = i;
        //         } else {
        //             start = total + 1;
        //         }
        //         if let RedisValueRef::Int(j) = arr[3] {
        //             end = j;
        //         } else {
        //             end = total + 1;
        //         }

        //         if start.abs() > total || end.abs() > total {
        //             println!("if case")
        //         } else {
        //             if start < 0 && start.abs() <= total {
        //                 start = total + start;
        //             }
        //             if end < 0 && end.abs() <= total {
        //                 end = total + end;
        //             }
        //             println!("start :{:?} ,end :{:?}", start, end);
        //             if start < end {
        //                 list.make_contiguous();
        //                 let (front, back) = list.as_slices();
        //                 encode(
        //                     &RedisValueRef::Array(VecDeque::from(
        //                         front.to_vec()[start as usize..end as usize].to_owned(),
        //                     )),
        //                     &mut res,
        //                 );
        //             }
        //         }
        //     }
        //     println!("inside lrange");
        //     stream.write(&res).await.unwrap();
        // }

        // "LPOP" => {
        //     let mut ans = RedisValueRef::NullBulkString;
        //     let mut res = BytesMut::new();
        //     {
        //         let mut temp1 = temp2.lock().unwrap();
        //         let list2 = temp1.get_mut(&arr[1]).unwrap();
        //         if let value::Simple(list1) = list2 {
        //             if let RedisValueRef::Array(list2) = list1 {
        //                 match list2.pop_front() {
        //                     Some(num) => ans = num,
        //                     None => ans = RedisValueRef::NullBulkString,
        //                 }
        //             }
        //         }
        //     }
        //     encode(&ans, &mut res);
        //     stream.write(&res).await;
        // }

        // "BLPOP" => {
        //     let mut res = BytesMut::new();
        //     let mut res1 = RedisValueRef::NullArray;
        //     let mut time_required = 200 as u64;
        //     let curr = SystemTime::now();
        //     if let RedisValueRef::Int(t) = arr[1] {
        //         time_required = t as u64
        //     }

        //     loop {
        //         if SystemTime::now().duration_since(curr).unwrap().as_secs()
        //             > time_required
        //         {
        //             break;
        //         }
        //         {
        //             let mut temp1 = temp2.lock().unwrap();
        //             let temp = temp1.get_mut(&arr[1]).unwrap();
        //             let mut temp_list = VecDeque::new();
        //             let mut list = &mut temp_list;

        //             match temp {
        //                 value::Simple(val) => match val {
        //                     RedisValueRef::Array(real_list) => {
        //                         list = real_list;
        //                     }
        //                     _ => {}
        //                 },
        //                 value::Expiry((val, time)) => match val {
        //                     RedisValueRef::Array(real_list) => {
        //                         list = real_list;
        //                     }
        //                     _ => {}
        //                 },
        //             }
        //             if list.len() > 0 {
        //                 res1 = list.pop_front().unwrap();
        //                 break;
        //             }
        //         }
        //         tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        //     }
        //     encode(&res1, &mut res);
        //     stream.write(&res).await.unwrap();
        // }
        // "TYPE" => {
        //     let temp1 = temp2.lock().unwrap();
        //     let temp = temp1.get(&arr[1]).unwrap();
        //     //match value {}
        // }
        "XADD" => {
            XADD(arr, rt.clone(), last_stream_id_map.clone(), notify2.clone());
            // let mut temp1 = temp2.lock().unwrap();
            // let temp = temp1.get(&arr[1]);
            // match temp {
            //     Some(val) => {}
            //     None => {
            //         let mut map = BTreeMap::new();
            //         map.insert(
            //             RedisValueRef::String(Bytes::from(
            //                 "stream-key",
            //             )),
            //             RedisValueRef::String(Bytes::from(
            //                 "nothing for now",
            //             )),
            //         );
            //         temp1.insert(
            //             arr[1].clone(),
            //             value::Simple(RedisValueRef::Stream(map)),
            //         );
            //     }
            // }
        }
        "XRANGE" => {
            XRANGE(arr, rt.clone());
        }

        "XREAD" => {
            XREAD(arr, rt.clone(), last_stream_id_map.clone(), notify2.clone()).await;
        }

        "MULTI" => {
            MULTI(stack.clone(), arr);
        }

        "EXEC" => {
            EXEC(stream, data_structure, notify2).await;
        }
        _ => {}
    }
}

#[tokio::main]
async fn main() {
    println!("{:?}", Bytes::from("Hello, world!"));

    let listener = TcpListener::bind("127.0.0.1:9092").await.unwrap();
    let hash_map: Arc<Mutex<HashMap<RedisValueRef, value>>> = Arc::new(Mutex::new(HashMap::new()));
    let list: Arc<Mutex<VecDeque<RedisValueRef>>> = Arc::new(Mutex::new(VecDeque::new()));
    //let hash_set = Arc::new(Mutex::new(HashSet::new()));
    let radix_tree_root_node = radix_tree::RadixTreeNode {
        children: HashMap::new(),
        value: Some(HashMap::<String, String>::new()),
        end: false,
    };
    let radix_tree_map = Arc::new(Mutex::new(HashMap::new()));
    //let radix_tree = Arc::new(Mutex::new(radix_tree::RadixTree::new(radix_tree_root_node)));
    //let last_stream_id_map = Arc::new(Mutex::new(HashMap::new()));
    let notify = Arc::new(Notify::new());
    let data_structure = Arc::new(data_structure {
        rt: Arc::new(Mutex::new(HashMap::new())),
        set: Arc::new(Mutex::new(HashSet::new())),
        hashMap: Arc::new(Mutex::new((HashMap::new()))),
        last_stream_id_map: Arc::new(Mutex::new(HashMap::new())),
        stack: Arc::new(Mutex::new(Vec::new())),
    });

    loop {
        let mut stream = listener.accept().await.unwrap().0;
        let temp2 = hash_map.clone();
        let rt: Arc<Mutex<HashMap<String, RadixTree<HashMap<String, String>>>>> =
            radix_tree_map.clone();
        //let last_stream_id_map = last_stream_id_map.clone();
        let notify2 = notify.clone();
        //let hash_set2 = hash_set.clone();
        let data_structure = data_structure.clone();

        tokio::spawn(async move {
            //let mut buf = [0; 512];
            process(Some(stream), Some(data_structure), Some(notify2)).await;
        });
    }
}
