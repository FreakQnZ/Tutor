import os
import json
import whisper
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
AUDIO_READY_TOPIC = "audio_ready"
TRANSCRIPTION_READY_TOPIC = "transcription_ready"
# Load Whisper model once
print("Loading Whisper model...")
model = whisper.load_model("base")


def transcribe_audio(audio_path):
    print(f"Transcribing {audio_path}...")
    result = model.transcribe(audio_path)
    # segments = result["segments"]
    full_text = result["text"]

    # transcript = {
    #     "full_text": full_text,
    #     "segments": [
    #         {
    #             "start": seg["start"],
    #             "end": seg["end"],
    #             "text": seg["text"]
    #         } for seg in segments
    #     ]
    # }
    # return transcript
    return full_text


def start_consumer():
    consumer = KafkaConsumer(
        AUDIO_READY_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="audio_transcriber_group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"Listening to topic: {AUDIO_READY_TOPIC}")
    for message in consumer:
        try:
            audio_data = message.value
            audio_path = audio_data["audio_path"]
            video_id = audio_data.get("video_id", "unknown")
            print(f"Processing audio for video {video_id}")
            transcript = transcribe_audio(audio_path)

            payload = {
                "video_id": video_id,
                "audio_path": audio_path,
                "transcript": transcript
            }

            producer.send(TRANSCRIPTION_READY_TOPIC, payload)
            producer.flush()
            print(f"✅ Transcription for {video_id} sent to {TRANSCRIPTION_READY_TOPIC}")
            # print(f"Message recieved {message.value}")

        except Exception as e:
            print(f"❌ Error processing message: {e}")


if __name__ == "__main__":
    start_consumer()
