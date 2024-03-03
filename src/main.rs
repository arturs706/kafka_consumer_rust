use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use log::{info, warn};

struct CustomContext;

impl ClientContext for CustomContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = false;
}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

type LoggingConsumer = StreamConsumer<CustomContext>;

async fn consume_and_print(brokers: &str, group_id: &str, topic: &str) {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[topic])
        .expect("Can't subscribe to specified topic");

    loop {
        match consumer.recv().await {
            Err(e) => {
                warn!("Kafka error: {}", e);
                // Consumer might be disconnected if an error occurs
                consumer.commit_consumer_state(CommitMode::Async).unwrap();
            }
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                println!(
                    "key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                    m.key(),
                    payload,
                    m.topic(),
                    m.partition(),
                    m.offset(),
                    m.timestamp()
                );
                if let Some(headers) = m.headers() {
                    for header in headers.iter() {
                        println!("  Header {:#?}: {:?}", header.key, header.value);
                    }
                }
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}

#[tokio::main]
async fn main() {
    let topics = "register-landlords";
    let brokers = "localhost:9092";
    let group_id = "consumer-group-a";

    consume_and_print(brokers, group_id, topics).await
}

// use std::time::Duration;
// use log::info;
// use rdkafka::config::ClientConfig;
// use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
// use serde::{Serialize, Deserialize}; // Add these imports

// #[derive(Serialize, Deserialize)]
// struct DataStruct {
//     key: String,
//     payload: String,
// }

// async fn produce(key_acc: &str, payload: &str) -> Result<(), rdkafka::error::KafkaError> {
//   let brokers = "localhost:9092";
//   let topic_name = "register_landlords";
//     let producer: FutureProducer = ClientConfig::new()
//         .set("bootstrap.servers", brokers)
//         .set("message.timeout.ms", "5000")
//         .create()
//         .expect("Producer creation error");

//     // Create an instance of your data struct
//     let data = DataStruct {
//         key: key_acc.to_string(),
//         payload: payload.to_string(),
//     };

//     // Serialize your data to a JSON string
//     let json_data = serde_json::to_string(&data).expect("Failed to serialize data to JSON");

//     let record = FutureRecord::to(topic_name)
//         .payload(&json_data)
//         .key("key".as_bytes());

//     let (partition, offset) = producer
//         .send(record, Duration::from_secs(0))
//         .await.expect("Message failed to be produced");

//     info!(
//         "Published message at topic '{}' partition '{}' offset '{}'",
//         topic_name, partition, offset
//     );
//     producer.flush(Duration::from_secs(1)).expect("Flushing failed");
//     Ok(())
// }

// #[tokio::main]
// async fn main() {

// }
