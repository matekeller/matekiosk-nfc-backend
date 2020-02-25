use crossbeam_channel::{Receiver, Sender};
use parking_lot::Mutex;
use pcsc::{Context, MAX_BUFFER_SIZE, MAX_ATR_SIZE , Protocols, Scope, ShareMode };
use serde::Serialize;
use std::sync::Arc;
fn main() {
    let ctx = match Context::establish(Scope::User) {
        Ok(ctx) => ctx,
        Err(err) => {
            eprintln!("Failed to establish context: {}", err);
            std::process::exit(1);
        }
    };
    let mut sender = new_broadcaster();
    {
        let sender = sender.clone();
        std::thread::spawn(move || {
            let mut last_events = std::collections::HashMap::new();
            loop {
                std::thread::sleep(std::time::Duration::from_millis(100));
                // List available readers.
                let readers_buf = std::boxed::Box::leak(std::boxed::Box::new([0; 2048]));
                let readers = match ctx.list_readers(readers_buf) {
                    Ok(readers) => readers,
                    Err(err) => {
                        eprintln!("Failed to list readers: {}", err);
                        std::process::exit(1);
                    }
                };

                for reader in readers {
                    if !last_events.contains_key(reader) {
                        last_events.insert(reader, CardEvent::CardRemoved);
                    }
                    // Connect to the card.
                    let card = match ctx.connect(reader, ShareMode::Shared, Protocols::ANY) {
                        Ok(card) => card,
                        _ => {
                            update_and_send_event(&mut last_events ,CardEvent::CardRemoved, reader, sender.clone()); 
                            continue;
                        }
                    };

                    // Send an APDU command.
                    let apdu = b"\xFF\xCA\x00\x00\x00";
                    let mut rapdu_buf = [0; MAX_BUFFER_SIZE];
                    let mut rapdu = match card.transmit(apdu, &mut rapdu_buf) {
                        Ok(rapdu) => {
                            let mut rapdu =  hex::encode(rapdu);
                            let len = rapdu.len();
                            if len <= 4 {
                                update_and_send_event(&mut last_events, CardEvent::CardRemoved, reader, sender.clone());
                                continue;
                            }
                            rapdu.truncate(len - 4);
                            rapdu
                        },
                        _ => {
                            update_and_send_event(&mut last_events, CardEvent::CardRemoved, reader, sender.clone());
                            continue
                        }
                    };
                    let mut names_buf = [0; MAX_ATR_SIZE];
                    let mut atr_buf = [0; MAX_ATR_SIZE];
                    let card_id = match card.status2(&mut names_buf, &mut atr_buf) {
                        Ok(card_id) => card_id.atr(),
                        _ => {
                          update_and_send_event(&mut last_events, CardEvent::CardRemoved, reader, sender.clone());  
                          continue;
                        },
                    };
                    let event = match hex::encode(card_id).as_str() {
                        "3b8180018080" => CardEvent::CardPushed(format!("chip_{}",rapdu)),
                        "3b8980014d54434f537302020439" => {
                            rapdu.truncate(12);
                            CardEvent::CardPushed(format!("semesterticket_{}",rapdu))
                        },
                        "3b87800173c840000090006d" => CardEvent::CardPushed(format!("visa_{}", rapdu)),
                        "3b8f8001804f0ca000000306030001000000006a" => CardEvent::CardPushed(format!("blanks_{}", rapdu)),
                        "3b8f8001804f0ca0000003060300020000000069" => CardEvent::CardPushed(format!("uka_{}", rapdu)),
                        "3b8d80018073c021c057597562694b65ff7f" => CardEvent::CardPushed(format!("ubi_{}", rapdu)),
                        a => { eprintln!("{},{}", a, rapdu); CardEvent::CardRemoved},
                    };
                    update_and_send_event(&mut last_events, event, reader, sender.clone());
                }
            }
        });
    }

    // websocket stuff
    let server = std::net::TcpListener::bind("127.0.0.1:9001").unwrap();
    for stream in server.incoming() {
        use tungstenite::error::Error;
        let rec = sender.register_recv();
        let mut websocket = tungstenite::server::accept(stream.unwrap()).unwrap();
        std::thread::spawn(move || {
            loop {
                let event = match rec.recv() {
                    Ok(event) => event,
                    _ => continue,
                };
                match websocket.write_message(tungstenite::Message::Text(serde_json::to_string(&CardEventWrapper{event}).unwrap())) {
                    Err(Error::ConnectionClosed)|Err(Error::AlreadyClosed)|Err(Error::Io(_)) => break,
                    _ => (),
                }
            }
            websocket.close(None).ok();
        });
    }
}

fn update_and_send_event<'a, T>(hashmap: &mut std::collections::HashMap<&'a std::ffi::CStr, T>, event: T, reader: &'a std::ffi::CStr, sender: Broadcaster<T>) where T: Clone + Eq {
    hashmap.insert(reader, event.clone()).map(|last_event| {
        if last_event != event {
            sender.try_send(event);
        }
    });
}


#[derive(Serialize)]
struct CardEventWrapper {
    event: CardEvent,
}

#[derive(Clone, Debug,Eq,PartialEq,Serialize)]
enum CardEvent {
    CardPushed(String),
    CardRemoved,
}

#[derive(Clone, Debug)]
pub struct Broadcaster<T> {
    recievers: Arc<Mutex<Vec<Sender<T>>>>,
}

pub fn new_broadcaster<T>() -> Broadcaster<T> {
    Broadcaster{ recievers: Arc::new(Mutex::new(Vec::new())) }
}


impl<T> Broadcaster<T> where T: Clone{
    pub fn try_send(&self, message: T) {
        self.recievers.lock().iter().for_each(|rec| Broadcaster::try_send_ignoring_errors(rec, message.clone()));
    }

    pub fn register_recv(&mut self) -> Receiver<T> {
        let (sender, reciever) = crossbeam_channel::unbounded();
        self.recievers.lock().push(sender);
        reciever
    }

    fn try_send_ignoring_errors(sender: &Sender<T>, message: T) {
        sender.try_send(message).ok();
    }
}
