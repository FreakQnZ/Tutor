import os
import json
import logging
import pickle
from typing import Dict, List, Any, Optional
from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer
from langchain.text_splitter import RecursiveCharacterTextSplitter
from sentence_transformers import SentenceTransformer
from PIL import Image
import faiss
import numpy as np
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TRANSCRIPTION_READY_TOPIC = "transcription_ready"
PDF_READY_TOPIC = "pdf_ready"
EMBEDDINGS_READY_TOPIC = "embeddings_ready"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variables for models and buffer
print("Loading CLIP model...")
clip_model = SentenceTransformer('clip-ViT-B-32')
text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)
video_buffer = {}  # Buffer to store partial data for each video_id

# Create directories
embeddings_dir = Path("data/embeddings")
embeddings_dir.mkdir(parents=True, exist_ok=True)

def chunk_text(text: str) -> List[str]:
    """Split text into chunks using RecursiveCharacterTextSplitter"""
    chunks = text_splitter.split_text(text)
    return chunks

def create_text_embeddings(chunks: List[str]) -> np.ndarray:
    """Create embeddings for text chunks using CLIP"""
    embeddings = clip_model.encode(chunks)
    return embeddings

def create_image_embeddings(image_paths: List[str]) -> tuple:
    """Create embeddings for images using CLIP"""
    embeddings = []
    valid_paths = []

    for data in image_paths:
        try:
            img_path = data.get('path')
            image = Image.open(img_path).convert('RGB')
            embedding = clip_model.encode(image)
            embeddings.append(embedding)
            valid_paths.append(img_path)
            logger.info(f"Created embedding for image: {img_path}")
        except Exception as e:
            logger.error(f"Error processing image {img_path}: {str(e)}")
            continue

    if embeddings:
        return np.array(embeddings), valid_paths
    else:
        return np.array([]), []

def create_faiss_index(embeddings: np.ndarray, index_path: str) -> None:
    """Create and save FAISS index"""
    if len(embeddings) == 0:
        logger.warning("No embeddings to index")
        return

    # Create FAISS index
    dimension = embeddings.shape[1]
    index = faiss.IndexFlatIP(dimension)  # Inner product for cosine similarity

    # Normalize embeddings for cosine similarity
    faiss.normalize_L2(embeddings)
    index.add(embeddings)

    # Save index
    faiss.write_index(index, index_path)
    logger.info(f"FAISS index saved to: {index_path}")

def save_metadata(metadata: Any, metadata_path: str) -> None:
    """Save metadata using pickle"""
    with open(metadata_path, 'wb') as f:
        pickle.dump(metadata, f)
    logger.info(f"Metadata saved to: {metadata_path}")

def process_transcription(video_id: str, transcription: str) -> None:
    """Process transcription data"""
    logger.info(f"Processing transcription for video_id: {video_id}")

    # Chunk the transcription
    chunks = chunk_text(transcription)
    logger.info(f"Created {len(chunks)} chunks from transcription")

    # Create embeddings
    embeddings = create_text_embeddings(chunks)

    # Create FAISS index
    transcription_index_path = embeddings_dir / f"{video_id}_transcription.index"
    create_faiss_index(embeddings, str(transcription_index_path))

    # Save chunks metadata
    transcription_metadata_path = embeddings_dir / f"{video_id}_transcription_metadata.pkl"
    save_metadata(chunks, str(transcription_metadata_path))

    # Update buffer
    if video_id not in video_buffer:
        video_buffer[video_id] = {}

    video_buffer[video_id]['transcription_index'] = str(transcription_index_path)
    video_buffer[video_id]['transcription_metadata'] = str(transcription_metadata_path)

    logger.info(f"Transcription processing completed for video_id: {video_id}")

def process_pdf(video_id: str, extracted_text: str, image_paths: List[str]) -> None:
    """Process PDF extracted text and images"""
    logger.info(f"Processing PDF for video_id: {video_id}")

    # Chunk the extracted text
    chunks = chunk_text(extracted_text)
    logger.info(f"Created {len(chunks)} chunks from extracted text")

    # Create embeddings for text chunks
    text_embeddings = create_text_embeddings(chunks)

    # Create FAISS index for extracted text
    extracted_index_path = embeddings_dir / f"{video_id}_extracted.index"
    create_faiss_index(text_embeddings, str(extracted_index_path))

    # Save extracted text chunks metadata
    extracted_metadata_path = embeddings_dir / f"{video_id}_extracted_metadata.pkl"
    save_metadata(chunks, str(extracted_metadata_path))

    # Process images
    image_embeddings, valid_image_paths = create_image_embeddings(image_paths)

    images_index_path = None
    images_metadata_path = None

    if len(image_embeddings) > 0:
        # Create FAISS index for images
        images_index_path = embeddings_dir / f"{video_id}_images.index"
        create_faiss_index(image_embeddings, str(images_index_path))

        # Save image paths metadata
        images_metadata_path = embeddings_dir / f"{video_id}_images_metadata.pkl"
        save_metadata(valid_image_paths, str(images_metadata_path))

        logger.info(f"Processed {len(valid_image_paths)} images")

    # Update buffer
    if video_id not in video_buffer:
        video_buffer[video_id] = {}

    video_buffer[video_id]['extracted_index'] = str(extracted_index_path)
    video_buffer[video_id]['extracted_metadata'] = str(extracted_metadata_path)

    if images_index_path:
        video_buffer[video_id]['images_index'] = str(images_index_path)
        video_buffer[video_id]['images_metadata'] = str(images_metadata_path)
        # print(video_buffer[video_id]['images_index'])
        # print()
        # print(video_buffer[video_id]['images_metadata'])

    logger.info(f"PDF processing completed for video_id: {video_id}")

def check_and_send_complete_payload(video_id: str, producer: KafkaProducer) -> None:
    """Check if both transcription and PDF are processed, then send to embeddings_ready"""
    if video_id not in video_buffer:
        return

    buffer_data = video_buffer[video_id]

    # Check if both transcription and extracted text are processed
    required_keys = ['transcription_index', 'extracted_index']

    if all(key in buffer_data for key in required_keys):
        logger.info(f"Both transcription and PDF processed for video_id: {video_id}")

        # Prepare payload
        payload = {
            'video_id': video_id,
            'transcription_index': buffer_data['transcription_index'],
            'transcription_metadata': buffer_data['transcription_metadata'],
            'extracted_index': buffer_data['extracted_index'],
            'extracted_metadata': buffer_data['extracted_metadata']
        }

        # Add images data if available
        if 'images_index' in buffer_data:
            payload['images_index'] = buffer_data['images_index']
            payload['images_metadata'] = buffer_data['images_metadata']

        # Send to embeddings_ready topic
        producer.send(EMBEDDINGS_READY_TOPIC, payload)
        producer.flush()

        # Remove from buffer
        del video_buffer[video_id]

        logger.info(f"✅ Complete payload sent to {EMBEDDINGS_READY_TOPIC} for video_id: {video_id}")

def start_consumer():
    """Main consumer function"""
    consumer = KafkaConsumer(
        TRANSCRIPTION_READY_TOPIC,
        PDF_READY_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="embedding_worker_group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"Listening to topics: {TRANSCRIPTION_READY_TOPIC}, {PDF_READY_TOPIC}")

    for message in consumer:
        try:
            topic = message.topic
            data = message.value

            video_id = data.get('video_id')
            if not video_id:
                logger.error("No video_id found in message")
                continue

            print(f"Processing message from topic: {topic} for video_id: {video_id}")

            if topic == TRANSCRIPTION_READY_TOPIC:
                transcription = data.get('transcript')  # Note: your transcriber uses 'transcript'
                if transcription:
                    process_transcription(video_id, transcription)
                    check_and_send_complete_payload(video_id, producer)
                else:
                    logger.error("No transcript found in message")

            elif topic == PDF_READY_TOPIC:
                extracted_text = data.get('extracted_text')
                image_paths = data.get('images', [])

                if extracted_text:
                    process_pdf(video_id, extracted_text, image_paths)
                    check_and_send_complete_payload(video_id, producer)
                else:
                    logger.error("No extracted_text found in message")

            else:
                logger.warning(f"Unknown topic: {topic}")

        except Exception as e:
            print(f"❌ Error processing message: {e}")
            logger.error(f"Error details: {str(e)}")

if __name__ == "__main__":
    start_consumer()
