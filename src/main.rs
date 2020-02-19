use pcsc::{Context, MAX_BUFFER_SIZE, MAX_ATR_SIZE , Protocols, Scope, ShareMode };
use serde::Serialize;

#[tokio::main]
async fn main() {
    let ctx = match Context::establish(Scope::User) {
        Ok(ctx) => ctx,
        Err(err) => {
            eprintln!("Failed to establish context: {}", err);
            std::process::exit(1);
        }
    };
    let (sender, _) = tokio::sync::broadcast::channel(10);
    {
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
            let sender = sender.clone();
            let ctx = ctx.clone();
            std::thread::spawn(move || {
                let mut event = CardEvent::CardRemoved;
                let mut last_event = CardEvent::CardRemoved;
                loop {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    if event != last_event {
                        sender.send(event.clone()).ok();
                    }
                    last_event = event;
                    // Connect to the card.
                    let card = match ctx.connect(reader, ShareMode::Shared, Protocols::ANY) {
                        Ok(card) => card,
                        _ => {
                            event = CardEvent::CardRemoved;
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
                                event = CardEvent::CardRemoved;
                                continue;
                            }
                            rapdu.truncate(len - 4);
                            rapdu
                        },
                        _ => {
                            event = CardEvent::CardRemoved;
                            continue
                        }
                    };
                    let mut names_buf = [0; MAX_ATR_SIZE];
                    let mut atr_buf = [0; MAX_ATR_SIZE];
                    let card_id = match card.status2(&mut names_buf, &mut atr_buf) {
                        Ok(card_id) => card_id.atr(),
                        _ => {
                          event = CardEvent::CardRemoved;  
                          continue;
                        },
                    };
                    event = match hex::encode(card_id).as_str() {
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
                }
            });
        }
    }

    // websocket stuff
    let server = std::net::TcpListener::bind("127.0.0.1:9001").unwrap();
    for stream in server.incoming() {
        use tungstenite::error::Error;
        let mut rec = sender.subscribe();
        let mut websocket = tungstenite::server::accept(stream.unwrap()).unwrap();
        tokio::spawn(async move {
            loop {
                let event = match rec.recv().await {
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
