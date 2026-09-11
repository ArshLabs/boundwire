use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::protocol::{ErrorCode, PUBLISH_KIND, SUBSCRIBE_KIND, ServerMessage, Topic};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ConnectionId(pub u64);

#[derive(Clone)]
pub(crate) struct BrokerHandle {
    commands: mpsc::Sender<BrokerCommand>,
}

impl BrokerHandle {
    pub(crate) async fn register(
        &self,
        id: ConnectionId,
        request_id: u32,
        outgoing: mpsc::Sender<ServerMessage>,
        force_close: CancellationToken,
    ) -> Result<(), ()> {
        self.commands
            .send(BrokerCommand::Register {
                id,
                request_id,
                outgoing,
                force_close,
            })
            .await
            .map_err(|_| ())
    }

    pub(crate) async fn subscribe(
        &self,
        id: ConnectionId,
        request_id: u32,
        topic: Topic,
    ) -> Result<(), ()> {
        self.commands
            .send(BrokerCommand::Subscribe {
                id,
                request_id,
                topic,
            })
            .await
            .map_err(|_| ())
    }

    pub(crate) async fn publish(
        &self,
        id: ConnectionId,
        request_id: u32,
        topic: Topic,
        payload: Bytes,
    ) -> Result<(), ()> {
        self.commands
            .send(BrokerCommand::Publish {
                id,
                request_id,
                topic,
                payload,
            })
            .await
            .map_err(|_| ())
    }

    pub(crate) async fn remove(&self, id: ConnectionId) {
        let _ = self.commands.send(BrokerCommand::Remove { id }).await;
    }
}

pub(crate) fn spawn_broker(
    command_capacity: usize,
    shutdown: CancellationToken,
) -> (BrokerHandle, tokio::task::JoinHandle<()>) {
    let (commands, receiver) = mpsc::channel(command_capacity);
    let handle = BrokerHandle { commands };
    let task = tokio::spawn(Broker::new(receiver).run(shutdown));
    (handle, task)
}

enum BrokerCommand {
    Register {
        id: ConnectionId,
        request_id: u32,
        outgoing: mpsc::Sender<ServerMessage>,
        force_close: CancellationToken,
    },
    Subscribe {
        id: ConnectionId,
        request_id: u32,
        topic: Topic,
    },
    Publish {
        id: ConnectionId,
        request_id: u32,
        topic: Topic,
        payload: Bytes,
    },
    Remove {
        id: ConnectionId,
    },
}

struct Subscriber {
    topics: HashSet<Topic>,
    outgoing: mpsc::Sender<ServerMessage>,
    force_close: CancellationToken,
}

struct Broker {
    commands: mpsc::Receiver<BrokerCommand>,
    connections: HashMap<ConnectionId, Subscriber>,
    topics: HashMap<Topic, HashSet<ConnectionId>>,
    next_event_id: u64,
}

impl Broker {
    fn new(commands: mpsc::Receiver<BrokerCommand>) -> Self {
        Self {
            commands,
            connections: HashMap::new(),
            topics: HashMap::new(),
            next_event_id: 1,
        }
    }

    async fn run(mut self, shutdown: CancellationToken) {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                command = self.commands.recv() => match command {
                    Some(command) => self.handle(command),
                    None => break,
                }
            }
        }
    }

    fn handle(&mut self, command: BrokerCommand) {
        match command {
            BrokerCommand::Register {
                id,
                request_id,
                outgoing,
                force_close,
            } => self.register(id, request_id, outgoing, force_close),
            BrokerCommand::Subscribe {
                id,
                request_id,
                topic,
            } => self.subscribe(id, request_id, topic),
            BrokerCommand::Publish {
                id,
                request_id,
                topic,
                payload,
            } => self.publish(id, request_id, topic, payload),
            BrokerCommand::Remove { id } => self.remove_connection(id),
        }
    }

    fn register(
        &mut self,
        id: ConnectionId,
        request_id: u32,
        outgoing: mpsc::Sender<ServerMessage>,
        force_close: CancellationToken,
    ) {
        if let Some(previous) = self.connections.remove(&id) {
            previous.force_close.cancel();
            self.remove_memberships(id, previous.topics);
        }

        self.connections.insert(
            id,
            Subscriber {
                topics: HashSet::new(),
                outgoing,
                force_close,
            },
        );

        self.send_or_remove(
            id,
            ServerMessage::Ready {
                request_id,
                connection_id: id.0,
            },
        );
    }

    fn subscribe(&mut self, id: ConnectionId, request_id: u32, topic: Topic) {
        let Some(subscriber) = self.connections.get_mut(&id) else {
            return;
        };

        subscriber.topics.insert(topic.clone());
        self.topics.entry(topic).or_default().insert(id);

        self.send_or_remove(
            id,
            ServerMessage::Ack {
                request_id,
                acknowledged_kind: SUBSCRIBE_KIND,
                event_id: 0,
                matched: 0,
                enqueued: 0,
                evicted: 0,
            },
        );
    }

    fn publish(&mut self, publisher: ConnectionId, request_id: u32, topic: Topic, payload: Bytes) {
        if self.next_event_id == u64::MAX {
            self.send_or_remove(
                publisher,
                ServerMessage::Error {
                    request_id,
                    code: ErrorCode::EventIdExhausted,
                    detail: "event ID space is exhausted",
                },
            );
            return;
        }
        let event_id = self.next_event_id;
        self.next_event_id += 1;

        let recipients: Vec<_> = self
            .topics
            .get(&topic)
            .map(|connections| connections.iter().copied().collect())
            .unwrap_or_default();
        let matched = count(recipients.len());
        let mut enqueued = 0_usize;
        let mut removals = Vec::new();

        for id in recipients {
            let Some(subscriber) = self.connections.get(&id) else {
                removals.push(id);
                continue;
            };

            let message = ServerMessage::Event {
                event_id,
                topic: topic.clone(),
                payload: payload.clone(),
            };
            match subscriber.outgoing.try_send(message) {
                Ok(()) => enqueued += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    subscriber.force_close.cancel();
                    removals.push(id);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => removals.push(id),
            }
        }

        removals.sort_unstable_by_key(|id| id.0);
        removals.dedup();
        let evicted = removals.len();
        for id in removals {
            self.remove_connection(id);
        }

        self.send_or_remove(
            publisher,
            ServerMessage::Ack {
                request_id,
                acknowledged_kind: PUBLISH_KIND,
                event_id,
                matched,
                enqueued: count(enqueued),
                evicted: count(evicted),
            },
        );
    }

    fn send_or_remove(&mut self, id: ConnectionId, message: ServerMessage) {
        let failed = self.connections.get(&id).is_none_or(|subscriber| {
            match subscriber.outgoing.try_send(message) {
                Ok(()) => false,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    subscriber.force_close.cancel();
                    true
                }
                Err(mpsc::error::TrySendError::Closed(_)) => true,
            }
        });

        if failed {
            self.remove_connection(id);
        }
    }

    fn remove_connection(&mut self, id: ConnectionId) {
        if let Some(subscriber) = self.connections.remove(&id) {
            self.remove_memberships(id, subscriber.topics);
        }
    }

    fn remove_memberships(&mut self, id: ConnectionId, topics: HashSet<Topic>) {
        for topic in topics {
            let remove_topic = self.topics.get_mut(&topic).is_some_and(|connections| {
                connections.remove(&id);
                connections.is_empty()
            });
            if remove_topic {
                self.topics.remove(&topic);
            }
        }
    }
}

fn count(value: usize) -> u16 {
    u16::try_from(value).unwrap_or(u16::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Limits;

    #[tokio::test]
    async fn evicts_only_the_full_subscriber() {
        let limits = Limits::default();
        let shutdown = CancellationToken::new();
        let (broker, task) = spawn_broker(16, shutdown.clone());
        let topic = Topic::parse(b"blocks", &limits).unwrap();

        let (slow_tx, mut slow_rx) = mpsc::channel(1);
        let slow_force = CancellationToken::new();
        broker
            .register(ConnectionId(1), 1, slow_tx, slow_force.clone())
            .await
            .unwrap();
        assert!(matches!(
            slow_rx.recv().await,
            Some(ServerMessage::Ready { .. })
        ));
        broker
            .subscribe(ConnectionId(1), 2, topic.clone())
            .await
            .unwrap();
        assert!(matches!(
            slow_rx.recv().await,
            Some(ServerMessage::Ack { .. })
        ));

        let (fast_tx, mut fast_rx) = mpsc::channel(8);
        broker
            .register(ConnectionId(2), 3, fast_tx, CancellationToken::new())
            .await
            .unwrap();
        fast_rx.recv().await.unwrap();
        broker
            .subscribe(ConnectionId(2), 4, topic.clone())
            .await
            .unwrap();
        fast_rx.recv().await.unwrap();

        let (publisher_tx, mut publisher_rx) = mpsc::channel(8);
        broker
            .register(ConnectionId(3), 5, publisher_tx, CancellationToken::new())
            .await
            .unwrap();
        publisher_rx.recv().await.unwrap();

        broker
            .publish(
                ConnectionId(3),
                6,
                topic.clone(),
                Bytes::from_static(b"first"),
            )
            .await
            .unwrap();
        assert!(matches!(
            fast_rx.recv().await,
            Some(ServerMessage::Event { .. })
        ));
        publisher_rx.recv().await.unwrap();

        broker
            .publish(
                ConnectionId(3),
                7,
                topic.clone(),
                Bytes::from_static(b"second"),
            )
            .await
            .unwrap();
        assert!(matches!(
            fast_rx.recv().await,
            Some(ServerMessage::Event { .. })
        ));
        let ack = publisher_rx.recv().await.unwrap();
        assert!(matches!(ack, ServerMessage::Ack { evicted: 1, .. }));
        assert!(slow_force.is_cancelled());

        broker
            .publish(ConnectionId(3), 8, topic, Bytes::from_static(b"third"))
            .await
            .unwrap();
        fast_rx.recv().await.unwrap();
        let ack = publisher_rx.recv().await.unwrap();
        assert!(matches!(
            ack,
            ServerMessage::Ack {
                matched: 1,
                enqueued: 1,
                evicted: 0,
                ..
            }
        ));

        shutdown.cancel();
        task.await.unwrap();
    }
}
