use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};

use crate::network::client::SqlClient;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectivityState {
    Connected,
    Disconnected,
    Reconnecting,
}

#[allow(dead_code)]
pub struct ConnectivityMonitor {
    state: Arc<RwLock<ConnectivityState>>,
    target: String,
    interval: Duration,
}

impl ConnectivityMonitor {
    pub fn new<T: Into<String>>(target: T, interval: Duration) -> Self {
        Self {
            state: Arc::new(RwLock::new(ConnectivityState::Disconnected)),
            target: target.into(),
            interval,
        }
    }

    pub fn state(&self) -> Arc<RwLock<ConnectivityState>> {
        Arc::clone(&self.state)
    }

    pub async fn run(self) {
        let mut reconnecting = false;

        loop {
            let ok = Self::check_once(&self.target).await;

            let new_state = if ok {
                reconnecting = false;
                ConnectivityState::Connected
            } else if reconnecting {
                ConnectivityState::Reconnecting
            } else {
                reconnecting = true;
                ConnectivityState::Disconnected
            };

            {
                let mut guard = self.state.write().await;
                *guard = new_state;
            }

            sleep(self.interval).await;
        }
    }

    async fn check_once(target: &str) -> bool {
        let mut client = SqlClient::new(target);
        match client.execute_sql("SELECT 1").await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}
