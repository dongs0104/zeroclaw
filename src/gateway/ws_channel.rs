//! WebSocket-backed [`Channel`] for `ask_user` / `escalate_to_human` in gateway mode.
//!
//! `WsChannel` bridges the agent's channel-based tools with the WebSocket
//! connection to the browser.  Questions flow out via `outgoing_tx` and user
//! responses flow back in via `incoming_rx`.

use crate::channels::traits::{Channel, ChannelMessage, SendMessage};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

/// A [`Channel`] implementation that communicates over a WebSocket connection.
///
/// The channel does not own the WebSocket directly — instead it uses `mpsc`
/// channels that are drained/fed by the WebSocket message loop in `ws.rs`.
pub struct WsChannel {
    channel_name: String,
    /// Outgoing questions — picked up by the WS forward loop and sent to the client.
    outgoing_tx: mpsc::Sender<String>,
    /// Incoming responses — fed by the WS forward loop when the client replies.
    incoming_rx: Mutex<mpsc::Receiver<ChannelMessage>>,
}

impl WsChannel {
    /// Create a new WebSocket channel.
    ///
    /// * `outgoing_tx` — sender for questions destined for the browser client.
    /// * `incoming_rx` — receiver for user responses from the browser client.
    pub fn new(
        name: impl Into<String>,
        outgoing_tx: mpsc::Sender<String>,
        incoming_rx: mpsc::Receiver<ChannelMessage>,
    ) -> Self {
        Self {
            channel_name: name.into(),
            outgoing_tx,
            incoming_rx: Mutex::new(incoming_rx),
        }
    }
}

#[async_trait]
impl Channel for WsChannel {
    fn name(&self) -> &str {
        &self.channel_name
    }

    async fn send(&self, message: &SendMessage) -> anyhow::Result<()> {
        self.outgoing_tx
            .send(message.content.clone())
            .await
            .map_err(|e| anyhow::anyhow!("WsChannel send failed: {e}"))
    }

    async fn listen(
        &self,
        tx: mpsc::Sender<ChannelMessage>,
    ) -> anyhow::Result<()> {
        let mut rx = self.incoming_rx.lock().await;
        if let Some(msg) = rx.recv().await {
            tx.send(msg)
                .await
                .map_err(|e| anyhow::anyhow!("WsChannel listen forward failed: {e}"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_forwards_to_outgoing() {
        let (outgoing_tx, mut outgoing_rx) = mpsc::channel(8);
        let (_incoming_tx, incoming_rx) = mpsc::channel(8);
        let ch = WsChannel::new("ws", outgoing_tx, incoming_rx);

        let msg = SendMessage::new("Are you sure?", "user");
        ch.send(&msg).await.unwrap();

        let received = outgoing_rx.recv().await.unwrap();
        assert_eq!(received, "Are you sure?");
    }

    #[tokio::test]
    async fn listen_forwards_incoming_response() {
        let (outgoing_tx, _outgoing_rx) = mpsc::channel(8);
        let (incoming_tx, incoming_rx) = mpsc::channel(8);
        let ch = Arc::new(WsChannel::new("ws", outgoing_tx, incoming_rx));

        // Simulate a user response arriving
        incoming_tx
            .send(ChannelMessage {
                id: "resp_1".into(),
                sender: "user".into(),
                reply_target: "user".into(),
                content: "Yes, go ahead".into(),
                channel: "ws".into(),
                timestamp: 0,
                thread_ts: None,
                interruption_scope_id: None,
                attachments: vec![],
            })
            .await
            .unwrap();

        let (tx, mut rx) = mpsc::channel(1);
        ch.listen(tx).await.unwrap();

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.content, "Yes, go ahead");
    }

    #[test]
    fn channel_name() {
        let (outgoing_tx, _) = mpsc::channel(1);
        let (_, incoming_rx) = mpsc::channel(1);
        let ch = WsChannel::new("websocket", outgoing_tx, incoming_rx);
        assert_eq!(ch.name(), "websocket");
    }
}
