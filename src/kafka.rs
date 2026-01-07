use anyhow::{anyhow, Result};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use std::time::Duration;

pub struct KafkaRecord {
    pub topic: String,
    pub payload: Vec<u8>,
}

pub struct KafkaSource {
    consumer: BaseConsumer,
}

impl KafkaSource {
    pub fn new(
        brokers: &str,
        group_id: &str,
        topics: &[String],
        offset_reset: &str,
    ) -> Result<Self> {
        if topics.is_empty() {
            return Err(anyhow!("kafka topics cannot be empty"));
        }

        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", offset_reset)
            .create()?;

        let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
        consumer.subscribe(&topic_refs)?;

        Ok(Self { consumer })
    }

    pub fn poll(&self, timeout: Duration) -> Result<Option<KafkaRecord>> {
        match self.consumer.poll(timeout) {
            None => Ok(None),
            Some(Ok(msg)) => {
                let payload = match msg.payload() {
                    Some(payload) => payload,
                    None => return Ok(None),
                };
                Ok(Some(KafkaRecord {
                    topic: msg.topic().to_string(),
                    payload: payload.to_vec(),
                }))
            }
            Some(Err(err)) => Err(anyhow!("kafka poll error: {err}")),
        }
    }
}

pub fn topic_for_venue(venue: &str) -> Result<String> {
    match venue {
        "binance-futures" => Ok("binance-futures".to_string()),
        "okex-futures" => Ok("okex-swap".to_string()),
        "binance-margin" => Ok("binance".to_string()),
        "okex-margin" => Ok("okex".to_string()),
        _ => Err(anyhow!("unsupported venue: {venue}")),
    }
}

pub fn list_topics(brokers: &str, timeout: Duration) -> Result<Vec<String>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "crypto_mth_proxy_meta")
        .set("enable.auto.commit", "false")
        .create()?;

    let metadata = consumer.fetch_metadata(None, timeout)?;
    let mut topics = metadata
        .topics()
        .iter()
        .map(|topic| topic.name().to_string())
        .collect::<Vec<_>>();
    topics.sort();
    Ok(topics)
}
