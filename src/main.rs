use crossbeam_channel::{Receiver, Sender};
use parking_lot::Mutex;
use pcsc::{Context, MAX_BUFFER_SIZE, PNP_NOTIFICATION, Protocols, ReaderState, Scope, ShareMode, State};
use serde::Serialize;
use std::sync::Arc;

fn main() {
    let mut sender = new_broadcaster();
    {
        let sender = sender.clone();
        std::thread::spawn(move || {
            let ctx = Context::establish(Scope::User).expect("failed to establish context");
            let mut readers_buf = [0; 2048];
            let mut reader_states = vec![
                // Listen for reader insertions/removals, if supported.
                ReaderState::new(PNP_NOTIFICATION(), State::UNAWARE),
            ];
            loop {
                // Remove dead readers.
                fn is_dead(rs: &ReaderState) -> bool {
                    rs.event_state().intersects(State::UNKNOWN | State::IGNORE)
                }

                for rs in &reader_states {
                    if is_dead(rs) {
                        sender.try_send(CardEvent::CardRemoved);
                    }
                }
                reader_states.retain(|rs| !is_dead(rs));

                // Add new readers.
                let names = ctx.list_readers(&mut readers_buf).expect("failed to list readers");
                for name in names {
                    if !reader_states.iter().any(|rs| rs.name() == name) {
                        reader_states.push(ReaderState::new(name, State::UNAWARE));
                    }
                }

                // Update the view of the state to wait on.
                for rs in &mut reader_states {
                    rs.sync_current_state();
                }

                // Wait until the state changes.
                ctx.get_status_change(None, &mut reader_states).expect("failed to get status change");

                for rs in &reader_states {
                    if rs.name() != PNP_NOTIFICATION() {
                        let event = if rs.event_state().contains(State::EMPTY) {
                            CardEvent::CardRemoved
                        } else {

                            let card = match ctx.connect(rs.name(), ShareMode::Shared, Protocols::ANY) {
                                Ok(card) => card,
                                _ => {
                                    continue;
                                }
                            };

                            // send apdu command
                            let apdu = b"\xFF\xCA\x00\x00\x00";
                            let mut rapdu_buf = [0; MAX_BUFFER_SIZE];
                            let mut rapdu = match card.transmit(apdu, &mut rapdu_buf) {
                                Ok(rapdu) => {
                                    let mut rapdu =  hex::encode(rapdu);
                                    let len = rapdu.len();
                                    if len <= 4 {
                                        continue;
                                    }
                                    rapdu.truncate(len - 4);
                                    rapdu
                                },
                                _ => {
                                    continue
                                }
                            };
                            match hex::encode(rs.atr()).as_str() {
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
                            }
                        };
                        sender.try_send(event);
                    }
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
