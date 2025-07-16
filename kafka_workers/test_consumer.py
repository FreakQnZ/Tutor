import os
import json
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
AUDIO_READY_TOPIC="embeddings_ready"

producer = KafkaProducer(
    # bootstrap_servers="host.docker.internal:9092",
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def start_producer():
    try:
        # payload = {
        #     "file_id": "fed2b8da-4b6b-44dd-9cd5-6678fd4cb7d9",
        #     "audio_path": "data/audio/fed2b8da-4b6b-44dd-9cd5-6678fd4cb7d9.wav",
        #     "original_video": "data/input/lecture_sample.mp4"
        # }
        # payload = {
        #     "file_id": "new",
        #     "audio_path": "data/audio/new.wav",
        #     "original_video": "data/input/new.mp4"
        # }

        payload = {
            'video_id': '1234',
            'transcription_index': 'data/embeddings/1234_transcription.index',
            'transcription_metadata': 'data/embeddings/1234_transcription_metadata.pkl',
            'extracted_index': 'data/embeddings/1234_extracted.index',
            'extracted_metadata': 'data/embeddings/1234_extracted_metadata.pkl',
            'images_index': 'data/embeddings/1234_images.index',
            'images_metadata': 'data/embeddings/1234_images_metadata.pkl'
        }

        producer.send(AUDIO_READY_TOPIC, payload)
        print(f"Sent message to {AUDIO_READY_TOPIC}")
        producer.flush()
    except Exception as e:
        print(f"Error sending message: {e}")

if __name__ == "__main__":
    start_producer()
