from kafka import KafkaConsumer
import json
TOPIC = "embeddings_ready"
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="final_notes_worker_group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print(f"Listening for messages on {TOPIC}...")

for message in consumer:
    print(f"Received message: {message.value}")
