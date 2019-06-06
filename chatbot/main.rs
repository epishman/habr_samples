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
    rc::Rc,
};

const URL: &str = "127.0.0.1:7070";

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

#[derive(Default, Clone)]
struct UserData {
    sid: String,
    msg_in: String,
    msg_out: String,
    script: String,
}

type UserDataCell = Rc<RefCell<UserData>>;

struct UserSession {
    udata: UserDataCell,
    scenario: Pin<Box<dyn Generator<Yield = (), Return = ()>>>,
}

type UserSessions = HashMap<String, UserSession>;

fn create_scenario(udata: UserDataCell) -> impl Generator<Yield = (), Return = ()> {
    move || {
        let uname;
        let mut umood;

        udata.borrow_mut().msg_out = format!("Hi, what is you name ?");
        yield ();

        uname = udata.borrow().msg_in.clone();
        udata.borrow_mut().msg_out = format!("{}, how are you feeling ?", uname);
        yield ();

        'not_ok: loop {
            umood = udata.borrow().msg_in.clone();
            if umood.to_lowercase() == "ok" { break 'not_ok; }
            udata.borrow_mut().msg_out = format!("{}, think carefully, maybe you're ok ?", uname);
            yield ();

            umood = udata.borrow().msg_in.clone();
            if umood.to_lowercase() == "ok" { break 'not_ok; }
            udata.borrow_mut().msg_out = format!("{}, millions of people are starving, maybe you're ok ?", uname);
            yield ();
        }

        udata.borrow_mut().msg_out = format!("{}, good bye !", uname);
        return ();
    }
}


fn main() {
    let listener = match TcpListener::bind(URL) {
        Ok(l) => l,
        Err(e) => panic!("Unable to bind at {}\n{}", URL, e),
    };

    std::thread::spawn(|| open_browser());
    println!("OS: {}\nURL: http://{}", OS, URL);

    let mut sessions: UserSessions = HashMap::new();
    let mut rnd = rand::thread_rng();

    for stream in listener.incoming() {
        let mut stream = stream.unwrap();

        let mut udata: UserData = read_udata(&mut stream);
        let mut sid = udata.sid.clone();
        let session;

        if sid == "" { //new session
            sid = rnd.gen::<u64>().to_string();
            udata.sid = sid.clone();
            let udata_cell = Rc::new(RefCell::new(udata));
            sessions.insert(
                sid.clone(),
                UserSession {
                    udata: udata_cell.clone(),
                    scenario: Box::pin(create_scenario(udata_cell)),
                }
            );
            session = sessions.get_mut(&sid).unwrap();
        } 
        else {
            match sessions.get_mut(&sid) {
                Some(s) => {
                    session = s;
                    session.udata.replace(udata);
                }
                None => {
                    println!("unvalid sid: {}", &sid);
                    continue;
                }
            }
        }

        udata = match session.scenario.as_mut().resume() {
            GeneratorState::Yielded(_) => session.udata.borrow().clone(),
            GeneratorState::Complete(_) => {
                let mut ud = sessions.remove(&sid).unwrap().udata.borrow().clone();
                ud.script = format!("document.getElementById('form').style.display = 'none'");
                ud
            }
        };

        write_udata(&udata, &mut stream);
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


fn open_browser() {
    std::thread::sleep(std::time::Duration::from_millis(100));
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
}
