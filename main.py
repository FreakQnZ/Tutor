import subprocess
import uuid
import time
import os
import base64
from kafka.admin import KafkaAdminClient, NewTopic
import streamlit as st
from threading import Thread
from queue import Queue, Empty

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPICS = [
    'audio_ready',
    'transcription_ready',
    'pdf_ready',
    'embeddings_ready'
]

def create_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id='streamlit_app'
    )
    topic_objects = []
    for topic in TOPICS:
        topic_objects.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
    try:
        admin_client.create_topics(new_topics=topic_objects, validate_only=False)
        st.write("Kafka topics created successfully.")
    except Exception as e:
        if 'TopicExistsError' in str(e) or 'TopicAlreadyExistsError' in str(e):
            st.write("Some or all topics already exist.")
        else:
            st.error(f"Error creating topics: {e}")
    finally:
        admin_client.close()

def enqueue_output(stream, queue, prefix):
    """Read lines from stream and put them into queue with prefix."""
    for line in iter(stream.readline, b''):
        queue.put(f"{line.decode(errors='replace')}")
    stream.close()

def start_worker(script_name, args=None):
    """Start a python kafka_worker script with optional args, capture output"""
    cmd = ['python', f'kafka_workers/{script_name}']
    st.write(f"Starting worker: {script_name}")
    if args:
        cmd.extend(args)
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1
    )
    return process

def main():
    st.title("Kafka Pipeline Streamlit App")

    create_topics()

    unique_id = str(uuid.uuid4())
    st.write(f"Pipeline run ID: {unique_id}")

    # Queue and thread runners for capturing logs from all subprocesses
    log_queue = Queue()
    processes = []

    # Start consumers (no uuid argument)
    st.write("Starting Kafka consumers...")
    consumer_names = ['embedding_worker.py', 'audio_transcriber.py', 'local_attention.py']
    for consumer in consumer_names:
        p = start_worker(consumer)
        t = Thread(target=enqueue_output, args=(p.stdout, log_queue, consumer), daemon=True)
        t.start()
        processes.append((p, t))

    time.sleep(5)  # Give consumers time to initialize

    # Start producers (with uuid argument)
    st.write("Starting entry point producers...")
    producer_names = ['video_to_audio.py', 'pdf_extractor.py']
    for producer in producer_names:
        p = start_worker(producer, args=[unique_id])
        t = Thread(target=enqueue_output, args=(p.stdout, log_queue, producer), daemon=True)
        t.start()
        processes.append((p, t))
        time.sleep(5)

    # Display logs in real-time
    logs_placeholder = st.empty()
    all_logs = []

    with st.spinner("Running pipeline..."):
        while True:
            # Gather logs from queue
            try:
                while True:
                    line = log_queue.get_nowait()
                    all_logs.append(line)
            except Empty:
                pass

            if all_logs:
                # Show last 50 lines
                # logs_placeholder.text("\n".join(all_logs[-50:]))
                filtered_logs = [line for line in all_logs if line.startswith('CLOG')]
                logs_placeholder.text("\n".join(filtered_logs[-50:]))

            # Check if the 2 producers have finished (entry points)
            producers_running = any(
                p.poll() is None and p.args[1].endswith('.py') and p.args[1] in producer_names
                for (p, _) in processes
            )

            # The consumers run indefinitely, so we rely on output PDF existence for completion

            # Check for output PDF file existence
            output_pdf_path = f"outputs/{unique_id}.pdf"
            if os.path.exists(output_pdf_path):
                st.success("Output PDF created!")
                break

            time.sleep(0.5)

    # Show PDF
    if os.path.exists(output_pdf_path):
        with open(output_pdf_path, "rb") as f:
            pdf_bytes = f.read()
        b64_pdf = base64.b64encode(pdf_bytes).decode('utf-8')

        st.download_button(
            label="Download PDF",
            data=pdf_bytes,
            file_name=f"{unique_id}.pdf",
            mime="application/pdf"
        )

        pdf_display = f'<iframe src="data:application/pdf;base64,{b64_pdf}" width="700" height="900" type="application/pdf"></iframe>'
        st.components.v1.html(pdf_display, height=900)

    else:
        st.warning("Output PDF not found within time limit.")

    # Terminate all processes (kill consumers and producers)
    st.write("Terminating all subprocesses...")
    for (p, _) in processes:
        p.terminate()
    st.write("All subprocesses terminated.")

if __name__ == "__main__":
    main()
