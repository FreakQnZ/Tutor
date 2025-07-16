import subprocess
import os
import json
from kafka import KafkaProducer
from uuid import uuid4
from dotenv import load_dotenv
from kafka.errors import KafkaTimeoutError

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
AUDIO_READY_TOPIC = "audio_ready"

producer = KafkaProducer(
    # bootstrap_servers="host.docker.internal:9092",
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def video_to_audio_ffmpeg(video_path, output_dir="data/audio"):
    os.makedirs(output_dir, exist_ok=True)

    file_id = str(uuid4())
    audio_path = os.path.join(output_dir, f"{file_id}.wav")

    command = [
        "ffmpeg",
        "-i", video_path,
        "-vn",
        "-acodec", "pcm_s16le",
        "-ac", "1",
        "-ar", "16000",
        audio_path
    ]

    try:
        subprocess.run(command, check=True)
        print(f"Audio saved to {audio_path}")

        # Send message to Kafka
        payload = {
            "file_id": file_id,
            "audio_path": audio_path,
            "original_video": video_path,
            "video_id": "1234"
        }
        producer.send(AUDIO_READY_TOPIC, payload)
        print(f"Kafka message sent to topic '{AUDIO_READY_TOPIC}'")

    except subprocess.CalledProcessError as e:
        print("Error converting video to audio:", e)
    except KafkaTimeoutError as e:
            print("Kafka send timeout:", e)
    finally:
        producer.flush()
        producer.close()

# Manual test run
if __name__ == "__main__":
    video_file = "data/input/lecture_sample.mp4"  # Put your video here
    video_to_audio_ffmpeg(video_file)
