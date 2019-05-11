//[dependencies]
//serde_json = "1.0"

use std::collections::{HashMap, HashSet};
use std::process::exit;
use serde_json::Value;

const FILE_BUF_SIZE: usize = 65535;
const CHANNEL_BUF_SIZE: usize = 1000;
const THREAD_SLEEP: std::time::Duration = std::time::Duration::from_nanos(100);
const PRN_COUNT: usize = 100000;
const PRN_LINE: &str = "---------------------------------------------------\n";

//source data
#[derive(Default)]
struct DebtRec {
    company: String,
    phones: Vec<String>,
    debt: f64,
}

//result data
#[derive(Default)]
struct Debtor {
    companies: HashSet<String>,
    phones: HashSet<String>,
    debt: f64,
}
#[derive(Default)]
struct Debtors {
    all: Vec<Debtor>,
    index_by_phone: HashMap<String, usize>,
}

//for universal multithreading
enum SyncAsyncSender<T> {
    Sync(std::sync::mpsc::SyncSender<T>),
    Async(std::sync::mpsc::Sender<T>),
}
impl<T> SyncAsyncSender<T> {
    fn new(syncasync: usize) -> (SyncAsyncSender<T>, std::sync::mpsc::Receiver<T>) {
        match syncasync {
            1 => {
                let (send, recv) = std::sync::mpsc::sync_channel(CHANNEL_BUF_SIZE);
                (SyncAsyncSender::Sync(send), recv)
            }
            2 => {
                let (send, recv) = std::sync::mpsc::channel();
                (SyncAsyncSender::Async(send), recv)
            }
            _ => panic!("ERROR: sync|async must be 1 or 2 !"),
        }
    }

    fn try_send(&self, data: T) -> bool {
        match &self {
            SyncAsyncSender::Sync(chan) => chan.try_send(data).is_ok(),
            SyncAsyncSender::Async(chan) => chan.send(data).is_ok(),
        }
    }
}


fn main() {
    let mut result = Debtors::default();

    let mut t = 0;
    let mut tcount = 0;
    let mut syncasync = 0;
    let mut fnames = vec![];

    let args: Vec<String> = std::env::args().collect();
    for i in 1..args.len() {
        let arg = &args[i];
        if arg == "-t" {
            t = 1;
        }
        else if t == 1 {
            t = 2;
            tcount = match arg.parse() {
                Ok(n) => n,
                Err(_) => 0
            }
        }
        else if t == 2 {
            t = 3;
            syncasync = match arg.as_str() {
                "sync" => 1,
                "async" => 2,
                _ => 0
            }
        } else {
            fnames.push(arg);
        }
    }
    if fnames.len() == 0 || tcount > 0 && syncasync == 0 {
        println!("{}USAGE: jsonparse \"<file name>\" \"<file name>\"... -t <n> sync|async ...
        -t <n> - thread count, 0 means a single-threaded model
        sync - synchronize the threads and minimize memory usage
        async - do not synchronize the threads and unlimited memory usage", PRN_LINE);
        exit(-1);
    }

    for f in fnames {
        let resultpart = process_file(&f, tcount, syncasync);
        if result.all.len() == 0 {
            result = resultpart;
        } else {
            merge_result(resultpart, &mut result);
        }
    }

    for (di, d) in result.all.iter().enumerate() {
        println!("{}#{}: debt: {}", PRN_LINE, di, &d.debt);
        println!("companies: {:?}\nphones: {:?}", &d.companies, &d.phones);
    }
} 


fn process_file(fname: &str, tcount: usize, syncasync: usize) -> Debtors { 
    use std::io::prelude::*;

    let mut result = Debtors::default();

    println!("{}file {}:", PRN_LINE, fname);
    let tbegin = std::time::SystemTime::now();

    let mut file = match std::fs::File::open(fname) {
        Ok(f) => f,
        Err(e) => {
            println!("FILE OPEN ERROR: {}", e);
            return result;
        }
    };

    //start threads
    let mut channels: Vec<SyncAsyncSender<Vec<u8>>> = vec![];
    let mut threads = vec![];
    for tid in 0..tcount {
        let (send, recv) = SyncAsyncSender::new(syncasync);
        channels.push(send);
        threads.push(std::thread::spawn(move || process_thread(recv, tid)));
    }
    let mut tid = 0;

    let mut buf = [0; FILE_BUF_SIZE];
    let mut i0 = 0;
    let mut osave:Vec<u8> = vec![];

    let mut braces = 0;
    let mut quotes = false;
    let mut backslash = false;

    let mut allcou = 0;
    let mut errcou = 0;
    let mut prncou = 0;

    loop {
        let blen = match file.read(&mut buf) {
            //Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Ok(0) => break,
            Ok(blen) => blen,
            Err(e) => {
                println!("FILE READ ERROR: {}", e);
                return result;
            }
        };
        for i in 0..blen {
            let b = buf[i];

            if b == b'"' && !backslash {
                quotes = !quotes;
            }
            backslash = b == b'\\';

            if !quotes {
                if b == b'{' {
                    if braces == 0 {
                        i0 = i;
                    }
                    braces += 1;
                }
                else if b == b'}' {
                    braces -= 1;
                    if braces == 0 { //object formed !

                        let mut o = &buf[i0..i+1];
                        i0 = 0;
                        if osave.len() > 0 {
                            osave.extend_from_slice(o);
                            o = &osave;
                        }

                        // single thread
                        if tcount == 0 {
                            match serde_json::from_slice(o) {
                                Ok(o) => {
                                    process_object(&o, &mut result);
                                } Err(e) => {
                                    println!("JSON OR UTF8 ERROR: {}", e);
                                    errcou += 1;
                                }
                            }
                            if prncou < PRN_COUNT {
                                prncou += 1;
                            } else {
                                allcou += prncou;
                                prncou = 1;
                                println!("{}", allcou);
                            }
                        } 
                        // multithreading
                        else {
                            loop {
                                tid = if tid < tcount-1 {tid+1} else {0};
                                if channels[tid].try_send(o.to_vec()) {
                                    break;
                                } else {
                                    std::thread::sleep(THREAD_SLEEP);
                                }
                            }
                        }

                        osave.clear();
                    }
                }
            } 
        }
        if i0 > 0 {
            osave.extend_from_slice(&buf[i0..]);
            i0 = 0;
        }
    }
    allcou += prncou;

    //stop threads
    for tid in 0..tcount {
        while !channels[tid].try_send(vec![]) {
            std::thread::sleep(THREAD_SLEEP);
        }
    }

    // join threads
    for _ in 0..tcount {
        let (resultpart, allcoupart, errcoupart) = threads.pop().unwrap().join().unwrap();
        if result.all.len() == 0 {
            result = resultpart;
        } else {
            merge_result(resultpart, &mut result);
        }
        allcou += allcoupart;
        errcou += errcoupart;
    }

    println!("file {}: processed {} objects in {:?}s, {} errors", 
        fname, allcou, tbegin.elapsed().unwrap(), errcou
    );

    result
}


fn process_thread(chan: std::sync::mpsc::Receiver<Vec<u8>>, tid: usize) -> (Debtors, usize, usize) {
    let mut result = Debtors::default();
    let mut allcou = 0;
    let mut errcou = 0;
    let mut prncou = 0;
    let mut prntab = String::new();
    for _ in 0..(tid)*2 {prntab.push('\t')}

    loop {
        let o = match chan.recv() {
            Ok(o) => o,
            Err(e) => {
                println!("THREAD ERROR: {}", e);
                panic!();
            }
        };
        if o.len() == 0 {
            break;
        }
        match serde_json::from_slice(&o) {
            Ok(o) => {
                process_object(&o, &mut result);
            }
            Err(e) => {
                println!("JSON OR UTF8 ERROR: {}", e);
                errcou += 1;
            }
        }
        if prncou < PRN_COUNT {
            prncou += 1;
        } else {
            allcou += prncou;
            prncou = 1;
            println!("{}#{}: {}", prntab, tid, allcou);
        }
    }
    allcou += prncou;
    (result, allcou, errcou)
}


fn process_object(o: &Value, result: &mut Debtors) {
    let dr = extract_data(o);
    //println!("{} - {:?} - {}", &dr.company, &dr.phones, &dr.debt,);

    let di = match dr.phones.iter().filter_map(|p| result.index_by_phone.get(p)).next() {
        Some(i) => *i,
        None => {
            result.all.push(Debtor::default());
            result.all.len()-1
        }
    };
    let d = &mut result.all[di];
    d.companies.insert(dr.company);
    for p in &dr.phones {
        d.phones.insert(p.to_owned()); 
        result.index_by_phone.insert(p.to_owned(), di);
    }
    d.debt += dr.debt;
}


fn merge_result(part: Debtors, result: &mut Debtors) {
    for dr in part.all {
        let di = match dr.phones.iter().filter_map(|p| result.index_by_phone.get(p)).next() {
            Some(i) => *i,
            None => {
                result.all.push(Debtor::default());
                result.all.len()-1
            }
        };
        let d = &mut result.all[di];
        d.companies.union(&dr.companies);
        for p in &dr.phones {
            d.phones.insert(p.to_owned());
            result.index_by_phone.insert(p.to_owned(), di);
        }
        d.debt += dr.debt;
    }
}


fn extract_data(o: &Value) -> DebtRec {

    fn val2str(v: &Value) -> String {
        match v {
            Value::String(vs) => vs.to_owned(), //to avoid additional quotes
            _ => v.to_string()
        }
    }

    let mut dr = DebtRec::default();

    let c = &o["company"];
    dr.company = match c {
        Value::Object(c1) => match &c1["name"] {
            Value::String(c2) => c2.to_owned(),
            _ => val2str(c)
        },
        _ => val2str(c)
    };

    match &o["phones"] {
        Value::Null => {}
        Value::Array(pp) => dr.phones.extend(pp.iter().map(|p| val2str(p))),
        pp => dr.phones.push(val2str(&pp))
    }

    match &o["phone"] {
        Value::Null => {}
        p => dr.phones.push(val2str(&p))
    }

    dr.debt = match &o["debt"] {
        Value::Number(d) => d.as_f64().unwrap_or(0.0),
        Value::String(d) => d.parse::<f64>().unwrap_or(0.0),
        _ => 0.0
    };

    dr
}
