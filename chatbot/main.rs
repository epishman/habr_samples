#![feature(generators, generator_trait)]

extern crate rand;
use rand::Rng;

use std::{
    io::prelude::*,
    net::{TcpListener, TcpStream},
    env::consts::OS,
    process::Command,
    collections::HashMap,
    ops::{Generator, GeneratorState},
    pin::Pin,
    cell::RefCell,
};

const URL: &str = "127.0.0.1:8080";

const HTML: &str = r#"
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
    </head>
    <body>
        <div>
            {:msg_out}
        </div>
        <form id="form" method="post">
            <input name="sid" type="hidden" value="{:sid}"/>
            <input name="msg_in" type="text" placeholder="type text and press Enter" autofocus style="width: 50em"/>
        </form>
        <script>
            {:script}
        </script>
    </body> 
    </html>
"#;

#[derive(Default)]
struct UserData {
    sid: String,
    msg_in: String,
    msg_out: String,
    script: String,
}

struct UserSession {
    udata_cell: RefCell<UserData>,
    scenario: Pin<Box<dyn Generator<Yield = String, Return = String>>>,
}

type UserSessions = HashMap<String, UserSession>;


fn user_scenario() -> impl Generator<Yield = String, Return = String> {
    || {
        yield format!("what is your name?");
        yield format!("{}, how are you feeling?", "anon");
        return format!("{}, bye !", "anon");
    }
}


fn main() {
    let listener = match TcpListener::bind(URL) {
        Ok(l) => l,
        Err(e) => panic!("Unable to bind at {}\n{}", URL, e),
    };

    println!("OS: {}\nURL: http://{}", OS, URL);
    match OS {
        "linux" => {
            Command::new("xdg-open")
                .arg("http://".to_string() + URL)
                .output().expect("Unable to open browser");
        }
        "windows" => {
            Command::new("rundll32")
                .arg("url.dll,FileProtocolHandler")
                .arg("http://".to_string() + URL)
                .output().expect("Unable to open browser");
        }
        _ => {}
    }

    let mut sessions: UserSessions = HashMap::new();
    let mut rnd = rand::thread_rng();

    for stream in listener.incoming() {
        let mut stream = stream.unwrap();

        let mut udata = read_udata(&mut stream);
        let mut sid = udata.sid.clone();
        let mut session;
        let mut deleted_session;

        if sid == "" { //new session
            sid = rnd.gen::<u64>().to_string();
            udata.sid = sid.clone();
            sessions.insert(
                sid.clone(),
                UserSession {
                    udata_cell: RefCell::new(udata),
                    scenario: Box::pin(user_scenario())
                }
            );
            session = sessions.get_mut(&sid).unwrap();
        } 
        else {
            match sessions.get_mut(&sid) {
                Some(s) => {
                    session = s;
                    session.udata_cell.replace(udata);
                }
                None => {
                    println!("unvalid sid: {}", &sid);
                    continue;
                }
            }
        }

        match session.scenario.as_mut().resume() {
            GeneratorState::Yielded(m) => {
                session.udata_cell.borrow_mut().msg_out = m;
            }
            GeneratorState::Complete(m) => {
                deleted_session = sessions.remove(&sid).unwrap();
                session = &mut deleted_session;
                session.udata_cell.borrow_mut().msg_out = m;
                session.udata_cell.borrow_mut().script = "document.getElementById('form').style.display = 'none'".to_string();
            }
        };

        write_udata(&session.udata_cell.borrow(), &mut stream);
    }
}


fn read_udata(stream: &mut TcpStream) -> UserData {
    let mut udata = UserData::default();

    let mut buf = [0; 1024];
    let len = stream.read(&mut buf).unwrap();
    let req = String::from_utf8_lossy(&buf[..len]).to_string();

    if req.starts_with("POST /") {
        let isid = req.find("sid=").unwrap();
        let imsg = req.find("&msg_in=").unwrap();
        udata.sid = req[isid+4..imsg].to_string();
        udata.msg_in = req[imsg+8..].to_string();
    }

    udata
}


fn write_udata(udata: &UserData, stream: &mut TcpStream) {
    let mut resp = HTML
        .replace("{:sid}", &udata.sid)
        .replace("{:msg_out}", &udata.msg_out)
        .replace("{:script}", &udata.script);

    resp = format!("HTTP/1.1 200 OK\r\n\r\n{}", resp);

    stream.write(resp.as_bytes()).unwrap();
    stream.flush().unwrap();
}
