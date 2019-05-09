//[dependencies]
//serde_json = "1.0"

use std::collections::{HashMap, HashSet};
use serde_json::Value;

const FILE_BUFFER_SIZE: usize = 100000;
const CHANNEL_BUFFER_SIZE: usize = 1000;
const PRN_COUNT: i32 = 100000;
const PRN_LINE: &str = "------------------------------------------------------------------\n";

//source data
#[derive(Default)]
struct DebtRec {
    company: String,
    phones: Vec<String>,
    debt: f64
}

//result data
#[derive(Default)]
struct Debtor {
    companies: HashSet<String>,
    phones: HashSet<String>,
    debt: f64
}

#[derive(Default)]
struct Debtors {
    all: Vec<Debtor>,
    index_by_phone: HashMap<String, usize>
}


fn main() {
    let mut result = Debtors::default();

    let mut threadcount = -1;
    let mut fflag = 0;
    for arg in std::env::args() {
        if arg == "-t" {
            threadcount = 0;
        }
        else if threadcount == 0 {
            threadcount = match arg.parse() {
                Ok(n) => n,
                Err(_) => {
                    println!("ERROR: -t \"{}\" - must be an integer !", arg);
                    break;
                }
            }
        }
        else if threadcount > 0 {
            if arg == "-f" {
                fflag = 1;
            }
            else if fflag == 1 {
                fflag = 2;
                let resultpart = process_file(&arg, threadcount as usize);
                if result.all.len() == 0 {
                    result = resultpart;
                } else {
                    merge_result(resultpart, &mut result);
                }
            }
        }
    }

    for (di, d) in result.all.iter().enumerate() {
        println!("{}#{}: debt: {}", PRN_LINE, di, &d.debt);
        println!("companies: {:?}\nphones: {:?}", &d.companies, &d.phones);
    }

    if threadcount <= 0 || fflag < 2 {
        println!("USAGE: fastpivot -t <thread count> -f \"file name\" -f \"file name\" ...");
    }
} 


fn process_file(fname: &str, threadcount: usize) -> Debtors { 
    use std::io::prelude::*;

    println!("{}file {}:", PRN_LINE, fname);
    let tbegin = std::time::SystemTime::now();

    let mut file = match std::fs::File::open(fname) {
        Ok(f) => f,
        Err(e) => {
            println!("ERROR: {}", e);
            return Debtors::default();
        }
    };

    let mut buf = [0; FILE_BUFFER_SIZE];
    let mut i0 = 0;
    let mut osave:Vec<u8> = vec![];

    let mut braces = 0;
    let mut quotes = false;
    let mut backslash = false;

    let mut threads = vec![];
    let mut channels = vec![];
    for tid in 0..threadcount { //start all threads
        // let (send, recv) = std::sync::mpsc::channel();
        let (send, recv) = std::sync::mpsc::sync_channel(CHANNEL_BUFFER_SIZE);
        threads.push(std::thread::spawn(move || process_thread(recv, tid)));
        channels.push(send);
    }
    let mut tid = 0;

    loop {
        let blen = match file.read(&mut buf) {
            //Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Ok(0) => break,
            Ok(blen) => blen,
            Err(e) => {
                panic!(e);
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

                        // channels[tid].send(o.to_vec()).unwrap();
                        // tid = if tid == threadcount-1 {0} else {tid+1};
                        loop {
                            tid = if tid == threadcount-1 {0} else {tid+1};
                            if channels[tid].try_send(o.to_vec()).is_ok() {
                                break; 
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

    for tid in 0..threadcount { //stop all threads
        channels[tid].send(vec![]).unwrap();
    }

    let mut result = Debtors::default();
    let mut allcount0 = 0;
    let mut errcount0 = 0;

    for _ in 0..threadcount {
        let (resultpart, allcount, errcount) = threads.pop().unwrap().join().unwrap();
        if result.all.len() == 0 {
            result = resultpart;
        } 
        else {
            merge_result(resultpart, &mut result);
        }
        allcount0 += allcount;
        errcount0 += errcount;
    }

    println!("file {}: processed {} objects in {:?}s, {} errors", 
        fname, allcount0, tbegin.elapsed().unwrap(), errcount0
    );

    result
}


fn process_thread(channel: std::sync::mpsc::Receiver<Vec<u8>>, tid: usize) -> (Debtors, i32, i32) {
    let mut result = Debtors::default();
    let mut allcount = 0;
    let mut errcount = 0;
    let mut prncount = 0;
    let mut prntab = String::new();
    for _ in 0..(tid)*2 {prntab.push('\t')}

    loop {
        let o = channel.recv().unwrap();
        if o.len() == 0 {
            break;
        }
        match serde_json::from_slice(&o) {
            Ok(o) => {
                process_object(&o, &mut result);
            }
            Err(e) => {
                println!("JSON ERROR: {}:\n{:?}", e, std::str::from_utf8(&o));
                errcount +=1;
            }
        }
        prncount += 1;
        if prncount == PRN_COUNT {
            allcount += prncount;
            prncount = 0;
            println!("{}#{}: {}", prntab, tid, allcount);
        }
    }
    allcount += prncount;
    (result, allcount, errcount)
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
