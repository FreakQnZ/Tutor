import os
import json
import time
import logging
import pickle
from pathlib import Path
from kafka import KafkaConsumer
import faiss
import numpy as np
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from sentence_transformers import SentenceTransformer
from langchain.text_splitter import RecursiveCharacterTextSplitter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Image, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch
from PIL import Image as PILImage

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
EMBEDDINGS_READY_TOPIC = "embeddings_ready"
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "fallback")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create directories
outputs_dir = Path("outputs")
outputs_dir.mkdir(parents=True, exist_ok=True)

# Initialize Groq LLM
llm = ChatGroq(model="gemma2-9b-it", api_key=GROQ_API_KEY)
clip_model = SentenceTransformer('clip-ViT-B-32')
splitter = RecursiveCharacterTextSplitter(chunk_size=300, chunk_overlap=50)

def create_pdf(summary, video_id):
    output_path = outputs_dir / f"{video_id}.pdf"
    doc = SimpleDocTemplate(str(output_path), pagesize=A4)
    styles = getSampleStyleSheet()
    story = []

    for item in summary:
        if item['type'] == 'text':
            para = Paragraph(item['content'], styles['Normal'])
            story.append(para)
            story.append(Spacer(1, 0.2 * inch))

        elif item['type'] == 'image':
            img_path = item['content']
            if os.path.exists(img_path):
                try:
                    img = Image(img_path)
                    img.drawHeight = 3 * inch  # Resize for layout
                    img.drawWidth = 4 * inch
                    story.append(img)
                    story.append(Spacer(1, 0.3 * inch))
                except Exception as e:
                    story.append(Paragraph(f"[Image load failed: {img_path}], {str(e)}", styles['Italic']))
            else:
                story.append(Paragraph(f"[Image not found: {img_path}]", styles['Italic']))

    doc.build(story)
    print(f"CLOG ✅ PDF created: {output_path}")

def add_images(doc, metadata, similarity_threshold=0.3):
    text_chunks = splitter.split_text(doc)
    text_embeddings = clip_model.encode(text_chunks, convert_to_numpy=True)
    text_embeddings = text_embeddings / np.linalg.norm(text_embeddings, axis=1, keepdims=True)
    dimension = text_embeddings.shape[1]
    index = faiss.IndexFlatIP(dimension)
    index.add(text_embeddings)

    with open(metadata, "rb") as f:
        image_paths = pickle.load(f)

    image_matches = {}  # key: text_index, value: list of image paths

    for img_path in image_paths:
        image = PILImage.open(img_path).convert("RGB")
        img_embedding = clip_model.encode([image], convert_to_numpy=True)
        img_embedding = img_embedding / np.linalg.norm(img_embedding, axis=1, keepdims=True)

        # Find best matching text chunk
        distances, indices = index.search(img_embedding, k=1)
        best_match_idx = indices[0][0]
        best_score = distances[0][0]

        # Append image to that match's list
        if best_score >= similarity_threshold:
            image_matches.setdefault(best_match_idx, []).append(img_path)

    summary = []

    for i, chunk in enumerate(text_chunks):
        summary.append({'type': 'text', 'content': chunk})

        # If images matched this chunk, insert them right after
        if i in image_matches:
            for img_path in image_matches[i]:
                summary.append({'type': 'image', 'content': img_path})

    return summary

def llm_response(text):

    def make_prompt(b_text):
        prompt = f"""
            You are given educational content structured as follows:

            - Query: A concise, topic-specific sentence or paragraph that captures the core idea or concept
            - Match 1, Match 2, Match 3: Transcribed excerpts from a teacher’s lecture that expand on or relate to the Query.

            Your task is to generate clear, concise, and comprehensive course notes. Focus on the Query, but enhance it with meaningful insights, context, or examples from the Matches.

            Instructions:
            - Do not include any introductions, labels, or extra text — only the final generated course notes.
            - Be accurate, clear, and focused. Avoid redundancy or off-topic content.
            - Try to generate a paragraph per query and its matches

            Here is the input:

            {b_text}
        """

        return prompt

    def estimate_tokens(text):
        return int(len(text) / 4)

    # Split the text into chunks under the target token size
    def split_text_by_tokens(text, max_tokens=14000):
        splitter = RecursiveCharacterTextSplitter(chunk_size=max_tokens * 4, chunk_overlap=200)  # ~4 chars/token
        return splitter.split_text(text)

    # Main batching loop
    def batch_invoke_llm(llm, full_text, max_tokens_per_minute=3000):
        chunks = split_text_by_tokens(full_text, max_tokens=max_tokens_per_minute - 1000)  # reserve tokens for instructions
        responses = []
        print(f"Number of chunks is {len(chunks)}, with estimates tokens {estimate_tokens(full_text)}")
        for i, chunk in enumerate(chunks):
            prompt = make_prompt(chunk)
            with open('output_prompt.txt', 'a') as f:
                f.write(prompt)

            print(f"CLOG 🔹 Sending batch {i + 1}/{len(chunks)} (Estimated tokens: {estimate_tokens(prompt)})")
            response = llm.invoke(prompt)
            responses.append(response.content)
            # with open("llm.txt", "a") as file:
            #     file.write(response.content + '\n')
            if i < len(chunks) - 1:
                print("⏳ Waiting 10 seconds to respect TPM limit...")
                time.sleep(10)

        return ''.join(responses)
        # return


    # response = llm.invoke(prompt)
    responses = batch_invoke_llm(llm, text)
    return responses

def find_similarity(transcription_metadata, extracted_metadata, extracted_index):
    # Implement similarity finding logic here
    loaded_index = faiss.read_index(extracted_index)

    with open(transcription_metadata, "rb") as f:
        transcription_chunks = pickle.load(f)

    with open(extracted_metadata, "rb") as f:
        extracted_chunks = pickle.load(f)

    query_embeddings = clip_model.encode(transcription_chunks, convvert_to_numpy=True)
    top_k = 3

    distances, indices = loaded_index.search(query_embeddings, top_k)

    res = ""

    for i, query_chunk in enumerate(transcription_chunks):
        res += f"Query {i}: {query_chunk}\n"
        for j, index in enumerate(indices[i]):
            res += f"  Match {j}: {extracted_chunks[index]}\n"

    return res

def process_embeddings_payload(payload):
    video_id = payload['video_id']
    transcription_index = payload['transcription_index']
    transcription_metadata = payload['transcription_metadata']
    extracted_index = payload['extracted_index']
    extracted_metadata = payload['extracted_metadata']
    images_index = payload['images_index']
    images_metadata = payload['images_metadata']

    content = find_similarity(extracted_metadata, transcription_metadata, transcription_index)
    pre_doc = llm_response(content)
    # with open("llm.txt", "r") as file:
    #     pre_doc = file.read()
    summary = add_images(pre_doc, images_metadata)
    create_pdf(summary, video_id)



def start_consumer():
    """Main consumer function"""
    consumer = KafkaConsumer(
        EMBEDDINGS_READY_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="final_notes_worker_group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    print(f"CLOG Listening to topic: {EMBEDDINGS_READY_TOPIC}")

    # payload = {
    #     'video_id': '1234',
    #     'transcription_index': 'data/embeddings/1234_transcription.index',
    #     'transcription_metadata': 'data/embeddings/1234_transcription_metadata.pkl',
    #     'extracted_index': 'data/embeddings/1234_extracted.index',
    #     'extracted_metadata': 'data/embeddings/1234_extracted_metadata.pkl',
    #     'images_index': 'data/embeddings/1234_images.index',
    #     'images_metadata': 'data/embeddings/1234_images_metadata.pkl'
    # }
    # process_embeddings_payload(payload)

    for message in consumer:
        try:
            payload = message.value
            video_id = payload.get('video_id')

            if not video_id:
                logger.error("No video_id found in message")
                continue

            print(f"CLOG Processing final notes for video_id: {video_id}")
            process_embeddings_payload(payload)

        except Exception as e:
            print(f"❌ Error processing message: {e}")
            logger.error(f"Error details: {str(e)}")

if __name__ == "__main__":
    start_consumer()
