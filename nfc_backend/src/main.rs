#![warn(rust_2018_idioms)]
#![forbid(elided_lifetimes_in_paths, unsafe_code)]

use futures_util::sink::SinkExt;
use pcsc::{
	Context, Protocols, ReaderState, Scope, ShareMode, State, MAX_BUFFER_SIZE,
	PNP_NOTIFICATION
};
use serde::Serialize;
use std::{process::exit, time::Duration};
use tokio::{
	net::{TcpListener, TcpStream},
	signal,
	sync::broadcast::{self, Receiver, Sender},
	time::sleep
};
use tokio_tungstenite::tungstenite;

async fn ctrl_c_handler(tx: &'static Sender<Event>) {
	match signal::ctrl_c().await {
		Ok(()) => {
			tx.send(Event::SigTerm);
		},
		Err(err) => {
			eprintln!("Unable to listen for shutdown signal: {}", err);
			// we also shut down in case of error
		}
	}
	exit(0);
}

async fn keep_alive(tx: &'static Sender<Event>) {
	loop {
		tx.send(Event::KeepAlive);
		sleep(Duration::from_secs(10)).await;
	}
}

async fn smartcard_handler(tx: &'static Sender<Event>) {
	tokio::task::spawn_blocking(|| {
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
					tx.send(Event::CardRemoved);
				}
			}
			reader_states.retain(|rs| !is_dead(rs));

			// Add new readers.
			let names = ctx
				.list_readers(&mut readers_buf)
				.expect("failed to list readers");
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
			ctx.get_status_change(None, &mut reader_states)
				.expect("failed to get status change");

			for rs in &reader_states {
				if rs.name() != PNP_NOTIFICATION() {
					let event = if rs.event_state().contains(State::EMPTY) {
						Event::CardRemoved
					} else {
						let card = match ctx.connect(
							rs.name(),
							ShareMode::Shared,
							Protocols::ANY
						) {
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
								let mut rapdu = hex::encode(rapdu);
								let len = rapdu.len();
								if len <= 4 {
									continue;
								}
								rapdu.truncate(len - 4);
								rapdu
							},
							_ => continue
						};
						match hex::encode(rs.atr()).as_str() {
							"3b8180018080" => {
								Event::CardPushed(format!("chip_{}", rapdu))
							},
							"3b8980014d54434f537302020439"
							| "3b8980014d54434f53730205043e"
							| "3b8e80015644562d4b41204944333720464d6e" => {
								rapdu.truncate(12);
								Event::CardPushed(format!("semesterticket_{}", rapdu))
							},
							"3b87800173c840000090006d" => {
								Event::CardPushed(format!("visa_{}", rapdu))
							},
							"3b8780018031c073d631c023" => {
								Event::CardPushed(format!("geldkarte_{}", rapdu))
							},
							"3b8f8001804f0ca000000306030001000000006a" => {
								Event::CardPushed(format!("blanks_{}", rapdu))
							},
							"3b8f8001804f0ca0000003060300020000000069" => {
								Event::CardPushed(format!("uka_{}", rapdu))
							},
							"3b8d80018073c021c057597562694b65ff7f" => {
								Event::CardPushed(format!("ubi_{}", rapdu))
							},
							"3b8a80010031c173c8400000900090" => {
								Event::CardPushed(format!("pagopace_{}", rapdu))
							},
							a => {
								eprintln!("{},{}", a, rapdu);
								Event::CardRemoved
							}
						}
					};
					tx.send(event);
				}
			}
		}
	});
}

async fn accept_connection(stream: TcpStream, mut rx: Receiver<Event>) {
	let addr = stream
		.peer_addr()
		.expect("connected streams should have a peer address");
	let mut ws_stream = tokio_tungstenite::accept_async(stream)
		.await
		.expect("Error during the websocket handshake occurred");
	println!("New Connection from {}", addr);

	loop {
		let event = match rx.recv().await {
			Ok(event) => event,
			_ => continue
		};
		if event.is_term() {
			break;
		}
		match ws_stream
			.send(tungstenite::Message::Text(
				serde_json::to_string(&CardEvent { event }).unwrap()
			))
			.await
		{
			Err(tungstenite::Error::ConnectionClosed)
			| Err(tungstenite::Error::AlreadyClosed)
			| Err(tungstenite::Error::Io(_)) => break,
			_ => ()
		}
	}
	ws_stream.close(None).await;
}

async fn run() {
	let (tx, rx) = broadcast::channel(16);
	let tx = Box::leak(Box::new(tx));

	tokio::spawn(ctrl_c_handler(tx));
	tokio::spawn(keep_alive(tx));
	tokio::spawn(smartcard_handler(tx));

	let addr = "127.0.0.1:9001";
	let listener = TcpListener::bind(&addr)
		.await
		.unwrap_or_else(|err| panic!("Failed to listen on {addr}: {err}"));
	println!("Listening on {addr}");
	while let Ok((stream, _)) = listener.accept().await {
		tokio::spawn(accept_connection(stream, rx.resubscribe()));
	}
}

#[tokio::main]
async fn main() {
	run().await
}

#[derive(Serialize)]
struct CardEvent {
	event: Event
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
enum Event {
	CardPushed(String),
	CardRemoved,
	SigTerm,
	KeepAlive
}

impl Event {
	fn is_term(&self) -> bool {
		matches!(self, Self::SigTerm)
	}
}
