import os
import json
import uuid
import fitz  # PyMuPDF
# from PIL import Image
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
PDF_READY_TOPIC = "pdf_ready"
IMAGE_OUT_DIR = "data/images"

os.makedirs(IMAGE_OUT_DIR, exist_ok=True)


def extract_text_and_images(pdf_path):
    doc = fitz.open(pdf_path)
    full_text = ""
    images_info = []

    for page_index in range(len(doc)):
        page = doc.load_page(page_index)
        full_text += page.get_text()

        images = page.get_images(full=True)
        for img_index, img in enumerate(images):
            xref = img[0]
            base_image = doc.extract_image(xref)
            image_bytes = base_image["image"]
            image_ext = base_image["ext"]

            image_filename = f"{uuid.uuid4()}.{image_ext}"
            image_path = os.path.join(IMAGE_OUT_DIR, image_filename)

            with open(image_path, "wb") as f:
                f.write(image_bytes)

            images_info.append({
                "page": page_index + 1,
                "path": image_path,
                "ext": image_ext
            })

    return full_text.strip(), images_info


def publish_pdf_data(pdf_path):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    pdf_id = str(uuid.uuid4())
    text, images = extract_text_and_images(pdf_path)

    payload = {
        "pdf_id": pdf_id,
        "pdf_path": pdf_path,
        "extracted_text": text,
        "images": images,
        "video_id": "1234"
    }

    producer.send(PDF_READY_TOPIC, payload)
    producer.flush()
    print(f"✅ Published PDF data for {pdf_path} to {PDF_READY_TOPIC}")


if __name__ == "__main__":
    pdf_file = "data/input/sample.pdf"
    publish_pdf_data(pdf_file)
