import os
import json
import time
from openai import OpenAI
import tiktoken

# Initialize the OpenAI client
client = OpenAI(api_key="YOUR_API_KEY_HERE")


# File to store batch IDs
BATCH_ID_FILE = "batch_ids.txt"
# Token limit for batch processing per organization
TOTAL_ENQUEUED_TOKEN_LIMIT = 2_000_000
# Model used for token counting
MODEL_NAME = "gpt-4o-mini"

def num_tokens_from_messages(messages, model=MODEL_NAME):
    """Return the number of tokens used by a list of messages."""
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        encoding = tiktoken.get_encoding("o200k_base")
    
    tokens_per_message = 3
    tokens_per_name = 1
    num_tokens = 0
    for message in messages:
        num_tokens += tokens_per_message
        for key, value in message.items():
            num_tokens += len(encoding.encode(value))
            if key == "name":
                num_tokens += tokens_per_name
    num_tokens += 3  # every reply is primed with <|start|>assistant<|message|>
    return num_tokens

def count_tokens_in_file(file_path, model=MODEL_NAME):
    """Count the total number of tokens in a JSONL file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        total_tokens = 0
        for line in f:
            datum = json.loads(line)
            messages = datum["body"]["messages"]
            total_tokens += num_tokens_from_messages(messages, model=model)
        return total_tokens

def split_large_file(file_path, token_limit=TOTAL_ENQUEUED_TOKEN_LIMIT, model=MODEL_NAME):
    """Split a large JSONL file into smaller chunks that stay under the token limit."""
    with open(file_path, 'r', encoding='utf-8') as f:
        chunks = []
        current_chunk = []
        current_token_count = 0
        
        for line in f:
            datum = json.loads(line)
            messages = datum["body"]["messages"]
            line_token_count = num_tokens_from_messages(messages, model=model)
            
            # Check if adding this line would exceed the token limit
            if current_token_count + line_token_count > token_limit:
                # Save current chunk and reset
                chunks.append(current_chunk)
                current_chunk = []
                current_token_count = 0
            
            current_chunk.append(datum)
            current_token_count += line_token_count
        
        # Append the last chunk if it's not empty
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks

def write_chunks_to_files(chunks, original_file_name, output_folder):
    """Write the chunks to separate JSONL files."""
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    for i, chunk in enumerate(chunks):
        chunk_file_name = f"{os.path.splitext(original_file_name)[0]}_part_{i + 1}.jsonl"
        chunk_file_path = os.path.join(output_folder, chunk_file_name)
        
        with open(chunk_file_path, 'w', encoding='utf-8') as f:
            for datum in chunk:
                f.write(json.dumps(datum) + '\n')
        
        print(f"Created chunk file: {chunk_file_path}")
        yield chunk_file_path

def save_batch_id(batch_id):
    """Save a batch ID to a file."""
    if batch_id.startswith("batch_"):  # Ensure only batch IDs are saved
        with open(BATCH_ID_FILE, 'a') as f:
            f.write(batch_id + '\n')

def all_batches_completed():
    """Check if all batches in the batch ID file are completed."""
    if not os.path.exists(BATCH_ID_FILE):
        return True  # No batches to check, so consider it safe to proceed
    
    with open(BATCH_ID_FILE, 'r') as f:
        batch_ids = f.read().splitlines()
    
    for batch_id in batch_ids:
        if batch_id.startswith("batch_"):  # Check only valid batch IDs
            batch = client.batches.retrieve(batch_id)
            if batch.status != "completed":  # Access status as an attribute
                print(f"Batch {batch_id} is still {batch.status}. Waiting...")
                return False
    
    return True


def wait_for_all_batches_to_complete():
    """Wait until all batches in the batch ID file are completed."""
    while not all_batches_completed():
        time.sleep(60)  # Wait for 1 minute before checking again
    print("All batches are completed. Safe to continue uploading.")

# Folder paths
folder_path = "COPY AND PASTE YOUR INPUT FOLDER PATH"
output_folder = "THIS CAN BE ANYTHING"
token_limit = 2_000_000

# Main logic for uploading files
for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)
    
    if file_name.endswith(".jsonl"):
        token_count = count_tokens_in_file(file_path)
        
        if token_count < token_limit:
            wait_for_all_batches_to_complete()  # Ensure all batches are completed before proceeding
            
            # Upload as is
            with open(file_path, "rb") as file:
                batch_input_file = client.files.create(
                    file=file,
                    purpose="batch"
                )
                
                batch_response = client.batches.create(
                    input_file_id=batch_input_file.id,
                    endpoint="/v1/chat/completions",
                    completion_window="24h",
                    metadata={
                        "description": file_name
                    }
                )
                batch_id = batch_response.id  # Capture the correct batch ID (starts with 'batch-')
                save_batch_id(batch_id)  # Save the correct batch ID
            print(f"Uploaded {file_name} with {token_count} tokens.")
        else:
            # Split the file and upload chunks
            print(f"Splitting {file_name} due to token count ({token_count} tokens exceeding {token_limit} limit).")
            chunks = split_large_file(file_path, token_limit, model=MODEL_NAME)
            for chunk_file_path in write_chunks_to_files(chunks, file_name, output_folder):
                chunk_token_count = count_tokens_in_file(chunk_file_path)
                
                wait_for_all_batches_to_complete()  # Ensure all batches are completed before proceeding
                
                with open(chunk_file_path, "rb") as chunk_file:
                    batch_input_file = client.files.create(
                        file=chunk_file,
                        purpose="batch"
                    )
                    
                    batch_response = client.batches.create(
                        input_file_id=batch_input_file.id,
                        endpoint="/v1/chat/completions",
                        completion_window="24h",
                        metadata={
                            "description": os.path.basename(chunk_file_path)
                        }
                    )
                    batch_id = batch_response.id  # Capture the correct batch ID (starts with 'batch-')
                    save_batch_id(batch_id)  # Save the correct batch ID
                print(f"Uploaded {os.path.basename(chunk_file_path)}.")
