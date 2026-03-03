use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use zmq::Message;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum AssetType {
    Stream,
}

impl AssetType {
    pub fn as_str(&self) -> &'static str {
        match self {
            AssetType::Stream => "stream",
        }
    }
}

pub struct PublisherManager {
    context: zmq::Context,
    sockets: HashMap<String, zmq::Socket>,
    ipc_dir: PathBuf,
}

impl PublisherManager {
    pub fn new(ipc_dir: impl Into<PathBuf>) -> Result<Self> {
        let ipc_dir = ipc_dir.into();
        fs::create_dir_all(&ipc_dir)?;
        Ok(Self {
            context: zmq::Context::new(),
            sockets: HashMap::new(),
            ipc_dir,
        })
    }

    pub fn publish(
        &mut self,
        exchange: &str,
        symbol: &str,
        asset: AssetType,
        payload: Message,
    ) -> Result<()> {
        let symbol = symbol.trim().to_uppercase();
        if symbol.is_empty() {
            return Ok(());
        }

        let key = format!("{}|{}|{}", exchange, symbol, asset.as_str());
        if !self.sockets.contains_key(&key) {
            let socket = self.create_socket(exchange, &symbol, asset)?;
            self.sockets.insert(key.clone(), socket);
        }

        match self.sockets.get(&key) {
            Some(socket) => socket
                .send(payload, 0)
                .map_err(|err| anyhow!("zmq send failed: {err}")),
            None => Ok(()),
        }
    }

    fn create_socket(&self, exchange: &str, symbol: &str, asset: AssetType) -> Result<zmq::Socket> {
        let path = self.endpoint_path(exchange, symbol, asset)?;
        if path.exists() {
            let _ = fs::remove_file(&path);
        }

        let endpoint = format!("ipc://{}", path.display());
        let socket = self.context.socket(zmq::PUB)?;
        socket.bind(&endpoint)?;
        Ok(socket)
    }

    fn endpoint_path(&self, exchange: &str, symbol: &str, _asset: AssetType) -> Result<PathBuf> {
        let mut dir = self.ipc_dir.join(exchange);
        fs::create_dir_all(&dir)?;

        let filename = format!("{}.ipc", symbol);
        dir.push(filename);
        Ok(dir)
    }
}
