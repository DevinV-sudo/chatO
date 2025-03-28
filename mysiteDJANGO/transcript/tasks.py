#import celery module
from celery import shared_task

#import azure tools
from azure.storage.blob import BlobServiceClient
import azure
from django.conf import settings

#video-audio-text packages
from moviepy.editor import VideoFileClip
import whisper

#other misc. imports
import os
import re
import shutil
import logging
import time
from dotenv import load_dotenv
from celery.exceptions import SoftTimeLimitExceeded
import backoff

#for unique chunk_ids
from uuid import uuid4

#import ollama for embeddings
import ollama

#import numpy
import numpy as np

#import pinecone
from pinecone import Pinecone, ServerlessSpec

#logging for celery tasks
logger = logging.getLogger(__name__)

#create client
blob_service_client = BlobServiceClient.from_connection_string(settings.AZURE_CONNECTION_STRING)

#import openai dependencies
import openai
openai.api_key = settings.OPENAI_API_KEY
client = openai.OpenAI()

#tokenizer for whisper model
import tiktoken

#import audio splitting software
from pydub import AudioSegment

#pinecone auth services
pinecone_api_key = settings.PINECONE_API_KEY
pc = Pinecone(api_key=pinecone_api_key)

#importing pdf handling libraries
import pymupdf
import pytesseract
from PIL import Image
import pytesseract
from pdf2image import convert_from_path
import fitz
import subprocess

#llama parse
from llama_parse import LlamaParse
from llama_index.core import SimpleDirectoryReader

#chunking libraries
from langchain_text_splitters import MarkdownHeaderTextSplitter
from langchain_text_splitters import RecursiveCharacterTextSplitter

#markdown processing
import mistune
import requests

@shared_task(ack_late=True, bind=True)
def allocate_mp4_processing(self, class_name, MP4_files):
    container_client = blob_service_client.get_container_client(settings.AZURE_CONTAINER)
    processing_mp4_paths = []

    for blob in MP4_files:
        logger.info(f"Attempting to move {os.path.basename(blob)} to Proccessing\n")
        try:
            blob_name = os.path.basename(blob)
            destination_path = f"{class_name}/Processing/{blob_name}"
            source_blob_client = container_client.get_blob_client(blob)

            if not source_blob_client.exists():
                logger.warning(f"Source Blob Not Found: {blob}\n")
                continue

            #loading in the destination blob client
            dest_blob_client = container_client.get_blob_client(destination_path)

            #start copy proccess
            copy_operation = dest_blob_client.start_copy_from_url(source_blob_client.url)

            #wait for copy process
            while True:
                props = dest_blob_client.get_blob_properties()
                copy_status = props.copy.status

                if copy_status == "success":
                    logger.info(f"Successfully copied {blob} to {destination_path}\n")

                    #delete the original blob
                    source_blob_client.delete_blob()
                    logger.info(f"Deleted orginal blob from {blob}")
                    break

                elif copy_status in ["failed", "aborted"]:
                    logger.warning(f"Copy operation failed for {blob_name}. Status: {copy_status}")
                    break
                time.sleep(2)
            
            #append new blob path
            processing_mp4_paths.append(destination_path)
        
        except Exception as e:
            logger.error(f"Error moving {blob}: {e}")
    data = (class_name, processing_mp4_paths)
    return data


@shared_task(acks_late=True, bind=True)
def process_uploaded_files(self, data):
    #unpacking the data
    class_name, MP4_files = data
    container_client = blob_service_client.get_container_client(settings.AZURE_CONTAINER)
    temp_download_dir = f"temp/{class_name}"
    processed_mp3_files = []  # new name to avoid shadowing
    
    try:
        os.makedirs(temp_download_dir, exist_ok=True)
        
        for blob_name in MP4_files:  # iterate over the input files
            download_path = os.path.join(temp_download_dir, os.path.basename(blob_name))
            blob_client = container_client.get_blob_client(blob_name)
            logger.info(f"starting processing for {blob_name}")

            with open(download_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            
            base_filename = os.path.splitext(os.path.basename(blob_name))[0]
            output_audio_path = os.path.join(temp_download_dir, f"{base_filename}.mp3")
            
            try:
                video_clip = VideoFileClip(download_path)
                audio_clip = video_clip.audio
                audio_clip.write_audiofile(output_audio_path)
                audio_clip.close()
                video_clip.close()
                processed_mp3_files.append(output_audio_path)  # append to the new list
            except Exception as e:
                logger.info(f"Error processing video file {download_path}: {e}")
            finally:
                os.remove(download_path)
    except Exception as e:
        logger.info(f"Error processing uploaded files for {class_name}: {e}")
        raise
    
    logger.info("beginning whisper transcription")
    data = (class_name, processed_mp3_files, MP4_files)  # return the processed files
    logger.info(f"PROCESS FILES END - Returning data: {data}")

    return data

@shared_task(acks_late=True, bind=True)
def audio_chunking(self, data):
    logger.info(f"Chunking audio files\n")

    #unpack the data
    class_name, mp3_files, MP4_files = data

    #set the chunk size (ten minutes)
    chunk_size = 10 * 60

    #storage for chunks
    audio_chunks = []

    #create directory for audio chunks to land
    chunk_path = f"temp/{class_name}/Chunks/"
    os.makedirs(chunk_path, exist_ok=True)

    #iterate through mp3s and chunk each
    for file in mp3_files:
        #create file spcific sub directory for chunks to land
        sub_chunk_path = f"{os.path.splitext(os.path.basename(file))[0]}"
        sub_chunk_dir = os.path.join(chunk_path, sub_chunk_path)
        os.makedirs(sub_chunk_dir, exist_ok=True)

        #create an Audio Segment object
        audio_file = AudioSegment.from_mp3(file)

        #get length
        file_length = audio_file.duration_seconds
        logger.info(f"File length is {file_length / 60} minutes long\n")

        #calculate the number of chunks (10 minute chunks)
        num_chunks = int(file_length // chunk_size)
        logger.info(f"attempting to chunk {file} in to {num_chunks} partitions\n")

        #storage for file specfic chunks
        file_chunks = []
        for i in range(num_chunks):
            #convert into milliseconds
            start_ms = i * chunk_size * 1000
            end_ms = (i + 1) * chunk_size * 1000

            #check if last iteration
            if i == num_chunks -1:
                logger.info(f"Reached last chunk iteration checking for remainder\n")
                
                #if there exists more audio data, widen this last chunk
                if audio_file[end_ms:]:
                    logger.info(f"Remaining audio detected - concatenating chunks\n")
                    #set the end_ms to end of file
                    chunk = audio_file[start_ms:]
                else:
                    #if no remainder set chunk as usual
                    logger.info(f"No Remainder detected\n")
                    chunk = audio_file[start_ms:end_ms]
            else:
                #split audio file
                chunk = audio_file[start_ms:end_ms]

            #save the chunk
            file_name = f"Chunk_{i+1}.mp3"
            file_save_path = os.path.join(sub_chunk_dir, file_name)
            chunk.export(file_save_path, format="mp3")

            logger.info(f"Saved {file_name} to {file_save_path}\n")
            file_chunks.append(file_save_path)
        
        #append the chunks for said file to the outer storage
        audio_chunks.append(file_chunks)

    #return the data
    data = (class_name, audio_chunks, MP4_files)
    return data

#truncate to maximum token length
def truncate_to_token(text):
    #initialize tokenizer
    tokenizer = tiktoken.get_encoding("cl100k_base")

    #encode the prompt and take the last 224 tokens
    transcript_tokens = tokenizer.encode(text)
    truncated_tokens = transcript_tokens[-224:]

    #decode the truncated prompt
    decoded_prompt = tokenizer.decode(truncated_tokens)
    return decoded_prompt

@backoff.on_exception(backoff.expo, openai.OpenAIError, max_tries=5)
def transcribe_with_retry(audio_path, text=""):
    """Handles API retries with exponential backoff."""
    with open(audio_path, "rb") as audio_file:
        #migration to newest openai api 
        transcription = client.audio.transcriptions.create(
            model="whisper-1", 
            file=audio_file, 
            response_format="text",
            prompt = text
            )
    return transcription.strip()
### source blob not found

@shared_task(acks_late=True, bind=True)
def whisper_transcription(self, data):
    logger.info(f"WHISPER START - Received data type: {type(data)}")
    logger.info(f"WHISPER START - Raw data: {data}")
    
    class_name, mp3_files, MP4_files = data
    temp_transcript_dir = f"temp/{class_name}/transcripts"
    os.makedirs(temp_transcript_dir, exist_ok=True)
    
    #path storage for transcriptions
    transcript_files = []

    for idx, chunk_batch in enumerate(mp3_files, start=1):
        #create an output_txt file
        output_transcript = os.path.join(temp_transcript_dir, f"Recording_{idx}_transcription.txt")

        #storage for the prompt (reset with each path)
        previous_text = None

        #iterate through the batch
        for audio_path in chunk_batch:

            try:
                logger.info(f"Starting transcription for: {audio_path}")

                #if we have a prompt transcribe with prompt, else no
                if previous_text:
                    logger.info(f"Prompt identified - transcribing with prompt\n")
                    transcription_text = transcribe_with_retry(audio_path=audio_path, text=previous_text)

                if not previous_text:
                    logger.info(f"No prompt detected - continuing wihtout\n")
                    transcription_text = transcribe_with_retry(audio_path=audio_path)

                #set the previous text for next audio paths prompt
                previous_text = truncate_to_token(transcription_text)

                #check if valid transcription was generated
                if not transcription_text:
                    logger.error(f"Transcription could not be generated for {os.path.basename(audio_path)}\n")
                    continue

                #write response to file
                with open(output_transcript, "a") as f:
                    f.write(transcription_text + "\n\n")

                #log completion
                logger.info(f"Transcription finished for Chunk{os.path.basename(audio_path)}")

                #time pause between files
                time.sleep(30)

            #log open-ai specific errors
            except openai.OpenAIError as e:
                logger.error(f"OpenAI API error for {audio_path}: {e}")

            except Exception as e:
                logger.info(f"Error transcribing audio file {audio_path}: {e}")
        #After the batch has been appended to the ouput_transcript
        transcript_files.append(output_transcript)
    
    # Proceed to upload transcriptions
    logger.info("Uploading transcriptions to Azure blob storage")
    data = (class_name, transcript_files, MP4_files)
    return data
    
@shared_task(acks_late=True, bind=True)
def upload_transcriptions(self, data):
    class_name, transcript_files, MP4_files = data
    temp_download_dir = f'temp/{class_name}'
    transcript_paths = []

    try:
        container_client = blob_service_client.get_container_client(settings.AZURE_CONTAINER)
        
        for transcript_file in transcript_files:
            try:
                #first change - upload to transcriptions within sub blob
                blob_name = f"{class_name}/Transcriptions/{os.path.basename(transcript_file)}"
                logger.info(f"Moving to :{blob_name}\n")

                blob_client = container_client.get_blob_client(blob_name)

                with open(transcript_file, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                    transcript_paths.append(blob_name)
                    logger.info(f"Successfully Uploaded {os.path.basename(transcript_file)}")

                os.remove(transcript_file)  # Clean up local file after upload
            except Exception as e:
                logger.info(f"Error uploading {transcript_file}: {e}")
        
        #iterate through the source files
        for transcript_file in MP4_files:
            #moving the completed transcriptions from Processing to completed
            try:
                file_name = os.path.basename(transcript_file).replace("_transcription.txt", ".mp4")
                source_path = f"{class_name}/Processing/{file_name}"
                destination_path = f"{class_name}/Completed/{file_name}"

                #generate source blob client
                source_blob_client = container_client.get_blob_client(source_path)

                if not source_blob_client.exists():
                    logger.warning(f"Source Blob Not Found:{source_path}\n")
                    continue

                #generate destination blob client
                dest_blob_client = container_client.get_blob_client(destination_path)

                #start copy process
                copy_operation = dest_blob_client.start_copy_from_url(source_blob_client.url)

                #wait for copy process to finish
                while True:
                    props = dest_blob_client.get_blob_properties()
                    copy_status = props.copy.status

                    if copy_status == "success":
                        logger.info(f"Successfully Copied {file_name} to {destination_path}\n")

                        #delete the original blob
                        source_blob_client.delete_blob()
                        logger.info(f"Deleted original blob from {source_path}")
                        break

                    elif copy_status in ["failed", "aborted"]:
                        logger.warning(f"Copy operation failed for {blob_name}, Status:{copy_status}\n")
                        break
                    time.sleep(2)
            except Exception as e:
                logger.error(f"Error moving {file_name}: {e}")

        logger.info(f"Successfully uploaded {len(transcript_files)} transcripts for {class_name}")
    except Exception as e:
        logger.info(f"Error uploading transcripts for {class_name}: {e}")
        raise
    try:
        shutil.rmtree(temp_download_dir)
        logger.info(f"Temporary directory {temp_download_dir} cleared.")
    except Exception as e:
        logger.info(f"Error clearing temporary directory {temp_download_dir}: {e}")
        raise
    
    logger.info("Transcription Process Complete, Starting Parsing of Transcripts.\n")
    data = class_name, transcript_paths
    return data

def page_count(pymupdf_doc, curr_page_count):
    #simply take pymupdf document and count pages
    page_count=pymupdf_doc.page_count

    #compare with current page count
    new_page_count = page_count + curr_page_count

    #return the new count if less than max_pages
    return new_page_count if new_page_count < 1000 else -1

@shared_task(ack_late=True, bind=True)
def allocate_processing(self, data):
    logger.info(f"Allocating 1000 pages worth of data to process....\n")
    
    #unpack the data 
    class_name, PDF_files = data

    #intialize the client
    container_client = blob_service_client.get_container_client(settings.AZURE_CONTAINER)

    #intialize the temporary directory for the parsed files
    temp_part_dir = f"temp/{class_name}"
    os.makedirs(temp_part_dir, exist_ok=True)

    #paths to process blob
    process_paths = []

    #keep track of the running page count
    running_page_count = 0
    #base blob destination path
    base_dest = f"{class_name}/Processing/"

    try:
        #try iterating though blob "pdf" paths
        for blob in PDF_files:
            download_path = os.path.join(temp_part_dir, os.path.basename(blob))
            blob_client = container_client.get_blob_client(blob)

            try:
                with open(download_path, "wb") as download_file:
                    download_file.write(blob_client.download_blob().readall())

                #open the pdf as a pymupdf doc
                with pymupdf.open(download_path) as doc:
                    new_count = page_count(doc, running_page_count)

                    if new_count == -1:
                        logger.info(f"Skipping {blob}, exceeds max page limit.")
                        continue
                    
                    running_page_count = new_count

                    #set the destination path for selected blob
                    destination_path = os.path.join(base_dest, os.path.basename(blob))### EDIT
                    logger.info(f"Blob: {blob}\n\n")
                    logger.info(f"blob-base.name: {os.path.basename(blob)}\n\n")
                    logger.info(f"base_dest + blob(basename) = {destination_path}")
                    process_paths.append(destination_path)

                    #initialize destination blob client
                    destination_blob_client = blob_service_client.get_blob_client(container=settings.AZURE_CONTAINER, blob=destination_path)

                    #upload the file from the PDF blob
                    with open(download_path, "rb") as upload_file:
                        logger.info(f"Uploading Blob: {blob}")
                        destination_blob_client.upload_blob(upload_file, overwrite=True)

                    #delete the original file from the source
                    logger.info(f"Removing Blob From Source")
                    blob_client.delete_blob()

            except Exception as e:
                logger.error(f"Error uploading blob: {blob}, {e}")

    finally:
            # Clean up temporary directory
            logger.info(f"Cleaning up temporary directory: {temp_part_dir}")
            shutil.rmtree(temp_part_dir, ignore_errors=True)  

    #return "data"
    data = class_name, process_paths, temp_part_dir
    return data


@shared_task(ack_late=True, bind=True)
def process_pdfs(self, data):
    #unpacking data
    class_name, process_paths, temp_part_dir = data

    #start processing
    logger.info("Trimming Unusable Chapters")
    
    #Initialize container client
    container_client = blob_service_client.get_container_client(settings.AZURE_CONTAINER)

    #paths for processed pdfs
    processed_pdfs = []

    temp_part_dir = f"temp/{class_name}_PDFS"
    os.makedirs(temp_part_dir, exist_ok=True)

    try:
        for blob in process_paths:
            download_path = os.path.join(temp_part_dir, os.path.basename(blob))
            blob_client = container_client.get_blob_client(blob)

            try:
                with open(download_path, "wb") as download_file:
                    download_file.write(blob_client.download_blob().readall())

                with pymupdf.open(download_path) as doc:
                    toc=doc.get_toc()

                    if not toc:
                        processed_pdfs.append(download_path)
                        logger.info(f"Included entire document for {os.path.basename(download_path)}")
                        continue
            
                    intro_patterns = {
                        'introduction', 'preface', 'foreward', 'prologue',
                        'acknowledgements', 'dedication', 'abstract'
                    }
                    
                    end_patterns = {
                        'glossary', 'appendix', 'appendices', 'index', 'bibliography',
                        'references', 'notes', 'afterword', 'epilogue', 'conclusion',
                        'acknowledgements', 'about the author', 'endnotes'
                    }

                    chapter_start_index = None
                    for i, (level, title, page) in enumerate(toc):
                        lower_title = title.lower()
                    
                        if any(pattern in lower_title for pattern in intro_patterns):
                            continue
                        
                        chapter_start_index = i
                        break
                
                    chapter_end_index = None
                    for i, (level, title, page) in enumerate(reversed(toc)):
                        lower_title = title.lower()
                        if any(pattern in lower_title for pattern in end_patterns):
                            continue
                        chapter_end_index = len(toc) - 1 - i
                        break    
                    
                    if chapter_start_index is None or chapter_end_index is None:
                        processed_pdfs.append(download_path) 
                        logger.info(f"Included entire document for {os.path.basename(download_path)}")
                        continue
                    
                    first_chapter_page = toc[chapter_start_index][2] - 1
                    last_chapter_page = toc[chapter_end_index][2] -1

                    if chapter_end_index + 1 < len(toc):
                        last_chapter_page = toc[chapter_end_index+1][2]-2
                    
                    new_doc = pymupdf.open()
                    new_doc.insert_pdf(doc, from_page=first_chapter_page, to_page=last_chapter_page)
    
                    tmp_path = download_path + ".tmp"
                    new_doc.save(tmp_path)
                    new_doc.close()

                    os.replace(tmp_path, download_path)
                    processed_pdfs.append(download_path)

                    logger.info(f"Successfully trimmed {os.path.basename(download_path)}")
                    logger.info(f"Kept pages {first_chapter_page + 1} to {last_chapter_page + 1}")
                    logger.info(f"Starting from '{toc[chapter_start_index][1]}' to '{toc[chapter_end_index][1]}'")

            except Exception as e:
                logger.error(f"Failed to process {blob}: {e}")
                continue 
    
        logger.info(f"PDFS for {class_name} have been processed")

    except Exception as e:
        logger.error(f"An error occurred proccessing pdfs: {e}")
        raise
    
    data = class_name, processed_pdfs, temp_part_dir
    return data                         

def split_pdf(input_path, chunk_size=10):
    #open the document as pymudpdf
    doc = pymupdf.open(input_path)
    total_pages = doc.page_count

    #storage for output chunks
    chunk_paths = []

    #extract the original directory of the file
    base_dir = os.path.dirname(input_path)

    #extract file name
    file_name = os.path.splitext(os.path.basename(input_path))[0]

    #flag for sucessful chunking
    success = True
    #chunk the document
    for start_page in range(0, total_pages, chunk_size):
        end_page = min(start_page + chunk_size, total_pages)

        #define the file out path
        chunk_file_path = os.path.join(base_dir, f"{file_name}_chunk_{start_page}-{end_page}.pdf")

        #create new document for each chunk
        chunk_doc = pymupdf.open()
        try:
            chunk_doc.insert_pdf(doc, from_page=start_page, to_page=end_page-1)

            #save the chunk
            chunk_doc.save(chunk_file_path)
            chunk_paths.append(chunk_file_path)
        
        except Exception as e:
            logging.error(f"Error Splitting {file_name}:{e}")
            success = False

        #close the new document
        finally:
            chunk_doc.close()
    
    #close the original docuement
    doc.close()

    #remove the original document from the directory:
    if success and len(chunk_paths) > 0:
        try:
            os.remove(input_path)
            logging.info(f"Removed original file: {input_path}")
        
        except Exception as e:
            logging.error(f"Failed to remove the original file {input_path}: {e}")
    else:
        logging.error(f"Chunking failed for {input_path}, keeping original file.")

    #compress the chunked file name and the paths
    return chunk_paths if success else [input_path]

def requires_chunking(pdf_path, threshold=50):
    doc = pymupdf.open(pdf_path)
    total_pages = doc.page_count
    doc.close()

    return total_pages>threshold


@shared_task(ack_late=True, bind=True)
def chunk_pdfs(self, data):
    #unpack data
    class_name, processed_pdfs, temp_part_dir = data
    modified_pdf_paths = []
    chunked_file_names = []

    for pdf in processed_pdfs:
        base_name = os.path.splitext(os.path.basename(pdf))[0]
        
        if requires_chunking(pdf, threshold=50):
            logger.info(f"Chunking: {os.path.basename(pdf)}")
            try:
                chunk_paths = split_pdf(pdf, chunk_size=20)
                if chunk_paths:
                    modified_pdf_paths.extend(chunk_paths)
                    chunked_file_names.append(base_name)
                    logger.info(f"Added {len(chunk_paths)} chunks for: {base_name}")

                else:
                    logger.warning(f"Chunking failed for: {base_name}, keeping original")
                    modified_pdf_paths.append(pdf)
                    
            except Exception as e:
                logger.error(f"Error chunking {base_name}: {e}")
                modified_pdf_paths.append(pdf)
        
        else:
            logger.info(f"No Chunks Needed - Keeping Original:{os.path.basename(pdf)}")
            modified_pdf_paths.append(pdf)

    #compress results to send to llama-parse
    data = class_name, modified_pdf_paths, temp_part_dir

    #return the compressed results
    return data

@shared_task(ack_late=True, bind=True)
def llama_parse_batch(self, data):
    #unpack data
    class_name, processed_pdfs, temp_part_dir = data
   
    #define the parser
    parser = LlamaParse(
        api_key = settings.LLAMA_CLOUD_API_KEY,
        result_type="markdown",
        extract_layout=True,
        num_workers = 4,
        verbose=True
        
    )

    #intialize extractor
    file_extractor = {".pdf": parser}
    documents = []

    #use a set to avoid repeats
    successful_blob_names = set()
    error_blob_names = set()

    successful_extractions = {}

    #processed pdfs is file_paths to temp directory
    logger.info(f"Beginning parse of {len(processed_pdfs)} PDFs")
    
    for pdf_path in processed_pdfs:
        max_retries = 3
        retry_delay = 5
        base_name = os.path.basename(pdf_path)

        for attempt in range(max_retries):
            try:
                doc = SimpleDirectoryReader(input_files = [pdf_path],
                                            file_extractor=file_extractor).load_data()

                if doc:
                    successful_extractions[pdf_path] = doc[0]

                    #check if the parsed file is a chunk file:
                    match = re.match(r"(.+)_chunk_\d+-\d+\.pdf", base_name)
                    if match:
                        #if it is add just the source file name
                        source_file_name = match.group(1)+".pdf"
                        logger.info(f"adding chunk base file to success: {source_file_name}")
                        successful_blob_names.add(source_file_name)

                    else:
                        #if its not a chunk just add its base name
                        logger.info(f"adding file name to success: {base_name}")
                        successful_blob_names.add(base_name)

                    logger.info(f"Successfully extracted: {os.path.basename(pdf_path)}")
                    documents.extend(doc)
                    break
                
                else:
                    logger.info("Document Parsing Returned Empty")

            except Exception as e:
                logger.error(f"Error Processing {pdf_path}: {e}")
                
                if attempt < max_retries - 1:  # Don't sleep on the last attempt
                    wait_time = min(retry_delay * (2 ** attempt), 30)
                    logger.warning(f"Attempt {attempt + 1} failed for {pdf_path}: {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                
                else:
                    logger.error(f"All attempts failed for {pdf_path} after {max_retries} retries: {e}")
                    
                    #check for the chunked file names we may have to move the file name
                    error_match = re.match(r"(.+)_chunk_\d+-\d+\.pdf", base_name)
                    if error_match:
                        error_file_name = match.group(1)+".pdf"
                        error_blob_names.add(error_file_name)
                        logger.info(f"adding chunk source file to error: {error_file_name}")
                    else:
                        error_blob_names.add(base_name)
                        logger.info(f"adding file name to error {base_name}")
    
    #moving error blobs to the error outer blob in azure
    for error_blob in error_blob_names:
        
        #set up source and destination blob paths
        error_blob_path = f"{class_name}/Error/{error_blob}"
        source_blob_path = f"{class_name}/Processing/{error_blob}"

        #logging for testing
        logger.info(f"Moving Failed Parsed: {error_blob} from {source_blob_path} to {error_blob_path}")

        #move the blobs to the error outer blob
        try:
            #get source blob client
            source_blob_client = blob_service_client.get_blob_client(container=settings.AZURE_CONTAINER, blob=source_blob_path)
            
            #check to see if blob exists in source
            if not source_blob_client.exists():
                logger.warning(f"Source not found: {source_blob_path}")
                continue

            #load in destination blob client
            dest_blob_client = blob_service_client.get_blob_client(container=settings.AZURE_CONTAINER, blob=error_blob_path)

            #start upload process
            copy_operation = dest_blob_client.start_copy_from_url(source_blob_client.url)

            #wait for copy to complete:
            while True:
                props = dest_blob_client.get_blob_properties()
                copy_status = props.copy.status

                if copy_status == "success":
                    logger.info(f"Successfully copied {error_blob} to {error_blob_path}")

                    # Delete original blob from "Processing"
                    source_blob_client.delete_blob()
                    logger.info(f"Deleted original blob from {source_blob_path}")
                    break

                elif copy_status in ["failed", "aborted"]:
                    logger.warning(f"Copy operation failed for {error_blob}. Status: {copy_status}")
                    break

                time.sleep(2)

        except Exception as e:
            logger.error(f"Error moving {error_blob}: {e}")

    # moving successfully parsed files to azure
    mark_down_paths = []

    for pdf_path, doc in successful_extractions.items():
        try:
            #set up a local temp for the output
            local_md_dir = os.path.join(temp_part_dir, "markdown_outputs")
            os.makedirs(local_md_dir, exist_ok=True)

            #write the documents to file and upload as markdown to azure
            base_azure_path = f"{class_name}/Markdown/"
            container_client = blob_service_client.get_container_client(settings.AZURE_CONTAINER)

            file_name = os.path.basename(pdf_path).replace(".pdf", ".md")
            local_md_path = os.path.join(local_md_dir, file_name)
            
            # Write parsed output to local markdown file
            with open(local_md_path, "w", encoding="utf-8") as md_file:
                md_file.write(doc.text)
            
            # Upload markdown file to Azure Blob Storage
            azure_blob_path = os.path.join(base_azure_path, file_name)
            mark_down_paths.append(azure_blob_path)

            logger.info(f"Upload path for Azure:{azure_blob_path}\n")

            #reading the local file to the azure blob
            with open(local_md_path, "rb") as data:
                data_bytes = data.read()
                if len(data_bytes) == 0:
                    logger.error(f"File {local_md_path} read as empty before upload!")

                logger.info(f"Uploading:{file_name} to {azure_blob_path}\n")
                container_client.upload_blob(name=azure_blob_path, data=data_bytes, overwrite=True)

            #Succesfully uploaded
            logger.info(f"Uploaded {file_name} to Azure at {azure_blob_path}")
       
        except Exception as e:
            logger.error(f"Error uploading to Markdown: {e}")
    logger.info("Markdown upload process completed!")
    
    #move successful uploads to the completed blob
    completed_azure_path = f"{class_name}/Completed/"
    source_azure_path = f"{class_name}/Processing/"

    #iterate through success set
    for completed_blob in successful_blob_names:
        #concatentate paths
        dest_path =f"{completed_azure_path}{completed_blob}"
        source_path =f"{source_azure_path}{completed_blob}"

        #logging for testing
        logger.info(f"moving {completed_blob} to {dest_path} from {source_path}")

        try:
            #get source blob client
            source_blob_client = blob_service_client.get_blob_client(container=settings.AZURE_CONTAINER, blob=source_path)
            
            #check to see if blob exists in source
            if not source_blob_client.exists():
                logger.warning(f"Source not found: {source_path}")
                continue
            
            #load in destination blob client
            dest_blob_client = blob_service_client.get_blob_client(container=settings.AZURE_CONTAINER, blob=dest_path)

            #start upload process
            copy_operation = dest_blob_client.start_copy_from_url(source_blob_client.url)

            #wait for copy to complete:
            while True:
                props = dest_blob_client.get_blob_properties()
                copy_status = props.copy.status

                if copy_status == "success":
                    logger.info(f"Successfully copied {completed_blob} to {dest_path}")

                    # Delete original blob from "Processing"
                    source_blob_client.delete_blob()
                    logger.info(f"Deleted original blob from {source_path}")
                    break

                elif copy_status in ["failed", "aborted"]:
                    logger.warning(f"Copy operation failed for {completed_blob}. Status: {copy_status}")
                    break

                time.sleep(2)

        except Exception as e:
            logger.error(f"Error moving {completed_blob}: {e}")

    try:
        shutil.rmtree(temp_part_dir)
        logger.info(f"Successfully removed temporary directory: {temp_part_dir}")
    except Exception as e:
        logger.error(f"Error removing temporary directory {temp_part_dir}: {e}")

    logger.info(f"Successfully Parsed Current Processing Batch")

    data = (class_name, mark_down_paths)
    return data

@shared_task(ack_late=True, bind=True)
def create_pinecone_index(self, data):
    #unpack data
    class_name, mark_down_paths = data

    #generate index
    logger.info(f"[{self.request.id}] Creating Pinecone index for class: {class_name}")
    index_name = class_name.lower().replace("_", "-").replace(" ", "-").strip()
    try:
        existing_indexes = [index_info["name"] for index_info in pc.list_indexes()]
        logger.debug(f"Existing indexes: {existing_indexes}")

        if index_name not in existing_indexes:
            logger.debug(f"Creating new index {index_name} with dimension 768")
            pc.create_index(
                name=index_name,
                dimension=768,
                metric="cosine",
                spec=ServerlessSpec(cloud="aws", region="us-east-1"),
            )
            logger.info(f"[{self.request.id}] Created Pinecone index: {index_name}")

            while not pc.describe_index(index_name).status["ready"]:
                logger.debug(f"Waiting for index {index_name} to be ready...")
                time.sleep(1)

            index = pc.Index(index_name)
            stats = index.describe_index_stats()
            logger.info(f"[{self.request.id}] Pinecone index ready: {index_name}")
            logger.debug(f"Initial index stats: {stats}")
            
        else:
            logger.info(f"[{self.request.id}] Index already exists: {index_name}")
            index = pc.Index(index_name)
            stats = index.describe_index_stats()
            logger.debug(f"Existing index stats: {stats}")

    except Exception as e:
        logger.error(f"[{self.request.id}] Error creating Pinecone index: {e}", exc_info=True)
        raise
    
    data = (mark_down_paths, class_name, index_name)
    return data

def extract_source_doc_name(blob_path: str) -> str:
    '''
    Helper function, extracts the file name from the file path, then trims away the .md suffix,
    and the preceding chunking label (if present)
    '''

    #seperate the file name
    directory, file_name = os.path.split(blob_path)

    #chunk positional flag pattern
    chunk_pattern = r'_chunk_\d+-\d+\.md$'

    #remove the chunk flag
    if re.search(chunk_pattern, file_name):
        cleaned = re.sub(chunk_pattern, '', file_name)
    #if no chunk flag, remove file type suffix
    elif file_name.endswith('.md'):
        cleaned = file_name[:-3]
    #in the case neither exist return the file_name
    else:
        cleaned = file_name

    return cleaned

def generate_keywords(chunk_text: str) -> list[str]:
    '''
    This function is in charge of generating a list of keywords associated with a input text excerpt.
    it makes an API call to Open-AI and generates a list used for connecting passages via keyword similarity.
    '''

    #generate the prompt for extracting keywords
    prompt = (
            f"""
            You are extracting a list of prominent and general keywords from the following text excerpt.
            Provide the extracted keywords as a comma-separated list.

            ### Guidelines:
            - Output only a comma-separated list of relevant and broadly applicable keywords.
            - Ensure the keywords capture the core themes of the text excerpt.
            - The keywords should be general enough that similar passages may share some of these keywords.
            - **Ignore any text that appears to be part of tables or tabular data.**
            - ** No returned keywords should include special characters **
            - Avoid overly specific terms that apply only to narrow contexts.

            ### Example output:
            [pandas, dataframe, histogram, ethics]

            ### Text Excerpt:
            {chunk_text}
            """
        )
    
    try:
        #make the api call to OpenAI
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are an expert assistant that extracts key words from text."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=50,
            temperature=0.3
        )
    except Exception as e:
        logging.error(f"Error making OpenAI API call: {e}\n")
        return []
    
    #extract the text output
    response_text = response.choices[0].message.content.strip()

    #remove potential brackets or extra white space
    if response_text.startswith('[') and response_text.ends_with(']'):
        response_text = response_text[1:-1]

    #convert to a list
    response_list = [keyword.strip().lower() for keyword in response_text.split(',') if keyword.strip()]
    return response_list




@shared_task(ack_late=True, bind=True)
def markdown_chunk_embeddings(self, data):
    '''
    Perhaps add a checker for max-token context length
    add checker for instances with no headers
    better logic for minimum chunks, maybe concatenate
    '''
    #pull the model
    ollama.pull("nomic-embed-text")

    #Unpack the data
    mark_down_paths, class_name, index_name = data

    #generate the index
    index = pc.Index(index_name)

    #set up temp directory for chunking
    temp_dir = f"temp/{class_name}_MD/"
    os.makedirs(temp_dir, exist_ok=True)

    #storage for splits
    # splits = []

    #storage for metadata
    metadata_dict = {}

    #pull blobs from azure back to local
    for blob in mark_down_paths:
        #extract the source file name from the blob path
        source_name = extract_source_doc_name(blob)

        #check if exists in the dict, if not add the new source file name
        if source_name not in metadata_dict:
            logger.info(f"Source name not detected, adding source name: {source_name}\n")

            #initialize as a nested dictionary to store the accumulator, splits, and their count number
            metadata_dict[source_name] = {}

            #initialize accumulator at zero
            metadata_dict[source_name]["accumulator"] = 0

            #initialize empty list for splits
            metadata_dict[source_name]["splits"] = []

        else:
            logger.info(f"Source name detected, continuing processing.\n")

        #pull azure blob to local
        source_blob_client = blob_service_client.get_blob_client(container=settings.AZURE_CONTAINER, blob=blob)
        logger.info(f"retreiving {os.path.basename(blob)}")

        local_file_path = os.path.join(temp_dir, os.path.basename(blob))

        #download blob to local
        try:
            with open(local_file_path, "wb") as file:
                file.write(source_blob_client.download_blob().readall())
            logger.info(f"Downloaded {os.path.basename(blob)}\n")
            
        except Exception as e:
            logger.error(f"Error downloading {blob}: {e}")
            continue

        #open the local file
        with open(local_file_path, "r", encoding="utf-8") as f:
            document =  f.read()

        if not document:
            logger.warning(f"Skipping empty document: {os.path.basename(blob)}")
            continue

        try:
            #markdown parser
            parser = mistune.create_markdown(renderer=mistune.AstRenderer())
            ast = parser(document)

        except Exception as e:
            logger.error(f"Markdown Processing Failed for :{os.path.basename(blob)}, {e}")
            continue

        #extract headers
        headers = []
        for node in ast:
            if node["type"] == "heading":
                level = node["level"]
                text = node["children"][0]["text"]
                headers.append((f"{'#'*level}", text))
        
        if len(headers) == 0:
            logger.error(f"Document has no headers.\n")
            continue
        
        #intialize the splitter
        markdown_splitter = MarkdownHeaderTextSplitter(headers)
        md_header_splits = markdown_splitter.split_text(document)

        #minimum chunk length
        min_length = 25

        #filter the splits, and store and accumulate their structural count
        for split in md_header_splits:
            #check for minimum text length
            if len(split.page_content.strip()) >= min_length:

                #extract the current source file split count
                count = metadata_dict[source_name]["accumulator"]

                #store in sub dictionary 
                split_dict = {"count": count, "split": split}

                #store the split data
                metadata_dict[source_name]["splits"].append(split_dict)

                #accumulate the iterator
                metadata_dict[source_name]["accumulator"] += 1

        #extract the list of dictionaries for the filtered splits
        filtered_splits = metadata_dict[source_name]["splits"]

        if not filtered_splits:
            logger.warning(f"All splits are too small.\n")
            continue

    #get the metadata source name list
    source_name_list = metadata_dict.keys()
    logger.info(f"Extracted splits from {len(source_name_list)} unique documents:\n")
    total_splits = 0
    for name in source_name_list:
        num_records = len(metadata_dict[name]["splits"])
        total_splits += num_records
        logger.info(f"Source File: {name}, Number of Records: {num_records}\n")

    #log successfull completion of splits
    logger.info(f"Completed Document Splits - Proceding to Embedding Stage.")

    #start embedding logic here
    all_vectors = []

    #iterate through unique source file names
    for name in source_name_list:
        #extract the data from source name
        data = metadata_dict[name]["splits"]
        
        #iterate through the list of split dicts
        for split_dict in data:
            #seperate the structural number, and the split itself
            split = split_dict["split"]
            split_number = split_dict["count"]

            #extract the required metadata
            source_file = name
            chunk_text = split.page_content
            chunk_id = str(uuid4())

            #generate keywords metadata
            try:
                keywords = generate_keywords(chunk_text)
                logger.info(f"Successfully generated {len(keywords)} keywords for Document: {source_file} - Passage: {split_number}.\n")

            except Exception as e:
                logger.warning(f"Failed to generate key word list for Document:{source_file} - Passage:{split_number}.\n")

            try:
                #generate an embedding
                response = ollama.embeddings(model="nomic-embed-text", prompt = chunk_text)
                embedding = response.embedding

                #verify generated embedding is of valid format
                if not embedding or not isinstance(embedding, list) or not all(isinstance(x, (float, int)) for x in embedding):
                    logger.warning(f"Skipping Document: {source_file} - Passage: {split_number}: Invalid embedding format ({type(embedding)})")
                    continue

                #create the vector metadata (structural position: [source file, iteration number], text, and ID)
                metadata = {
                    "chunk_id": str(chunk_id),
                    "class_name": str(class_name),
                    "source_file": str(source_file),
                    "passage_number": int(split_number),
                    "keywords": keywords,
                    "text": chunk_text,
                }

                if metadata:
                    logger.info(f"Successfully generated metadata for Document: {source_file} - Passage: {split_number}.\n")
                else:
                    logger.warning(f"Could not generate meta data for Document: {source_file} - Passage: {split_number}.\n")

                #store the vector with recorded meta-data
                all_vectors.append({
                "id":chunk_id,
                "values":embedding,
                "metadata":metadata
                })
            
            #log exceptions to vector embedding logic
            except Exception as e:
                logger.error(f"Error Embedding Document: {source_file} - Passage: {split_number}.\n Skipping Chunk...\n")
                continue
    #log ratio of completions
    logger.info(f"Created {len(all_vectors)} Vectors out of {total_splits} chunks\n")

    #attempt to upsert vectors into vector database
    if all_vectors:
            try:
                logger.info(f"Uploading {len(all_vectors)} vectors to Pinecone...")
                index.upsert(vectors=all_vectors)  # Pass the entire list at once
                logger.info("Successfully upserted document embeddings")
            except Exception as e:
                logger.error(f"Error Uploading to Pinecone: {e}")

    #remove the temporary directory after upload
    try:
        shutil.rmtree(temp_dir)
        logger.info(f"Successfully removed temporary directory: {temp_dir}")
    
    except Exception as e:
        logger.error(f"Error removing temporary directory {temp_dir}: {e}")
                
    return None

    # #iterate through splits
    # for i, split in enumerate(splits):
    #     chunk_text = split.page_content
    #     chunk_id = str(uuid4())

    #     try:
    #         #currently failing in this block
    #         response = ollama.embeddings(model="nomic-embed-text", prompt = chunk_text)
    #         embedding = response.embedding

    #         if not embedding or not isinstance(embedding, list) or not all(isinstance(x, (float, int)) for x in embedding):
    #             logger.warning(f"Skipping chunk {i}: Invalid embedding format ({type(embedding)})")
    #             continue
             
    #         #create meta-data
    #         metadata = {
    #             "class_name":str(class_name),
    #             "file_type":"pdf",
    #             "chunk_id":str(chunk_id),
    #             "text": chunk_text,
    #         }

    #         if metadata:
    #             logger.info(f"Successfully generated chunk meta data for chunk {i}\n")

    #         else:
    #             logger.warning(f"Could not generate meta data for Chunk {i}\n")

    #         all_vectors.append({
    #             "id":chunk_id,
    #             "values":embedding,
    #             "metadata":metadata
    #         })
    #         #error embedding chunk 12-:
    #     except Exception as e:
    #             logger.error(f"Error imbedding chunk {i}\n")
    #             continue
        
    # logger.info(f"Created {len(all_vectors)} Vectors out of {len(splits)} chunks\n")

    # #attempt to upsert vectors into vector database
    # if all_vectors:
    #         try:
    #             logger.info(f"Uploading {len(all_vectors)} vectors to Pinecone...")
    #             index.upsert(vectors=all_vectors)  # Pass the entire list at once
    #             logger.info("Successfully upserted document embeddings")
    #         except Exception as e:
    #             logger.error(f"Error Uploading to Pinecone: {e}")

    # #remove the temporary directory after upload
    # try:
    #     shutil.rmtree(temp_dir)
    #     logger.info(f"Successfully removed temporary directory: {temp_dir}")
    
    # except Exception as e:
    #     logger.error(f"Error removing temporary directory {temp_dir}: {e}")
                
    # return None

@shared_task(ack_late=True, bind=True)
def transcription_chunk_embedding(self, data):
    #unpack data
    transcript_paths, class_name, index_name = data

    #initialize pinecone index
    index = pc.Index(index_name)
    
    #generate local directory
    temp_dir = f"temp/transcripts/{class_name}/"
    os.makedirs(temp_dir, exist_ok=True)

    #storage for doc partitions
    splits = []

    #First download the transcripts from blob storage to local
    for blob in transcript_paths:
        source_blob_client = blob_service_client.get_blob_client(container=settings.AZURE_CONTAINER, blob=blob)
        logger.info(f"retreiving {os.path.basename(blob)}")

        local_file_path = os.path.join(temp_dir, os.path.basename(blob))

        #download blob to local
        try:
            with open(local_file_path, "wb") as file:
                file.write(source_blob_client.download_blob().readall())
            logger.info(f"Downloaded {os.path.basename(blob)}\n")
            
        except Exception as e:
            logger.error(f"Error downloading {blob}: {e}")
            continue

        #open the local file
        with open(local_file_path, "r", encoding="utf-8") as f:
            document =  f.read()

        if not document:
            logger.warning(f"Skipping empty document: {os.path.basename(blob)}")
            continue

        #set up text splitter
        text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=500,
        chunk_overlap=20,
        length_function=len,
        is_separator_regex=False,   
        )

        #genereate documents
        docs = text_splitter.create_documents([document])
        if docs:
            logger.info(f"Document of length: {len(document)} Partioned into {len(docs)} Splits.\n")
        
        else:
            logger.warning(f"No partitions were created for doc: {os.path.basename(blob)}")
            continue

        #minimum chunk length
        min_length = 25

        #filter out any accidental small splits
        filtered_splits = [split for split in docs if len(split.page_content.strip()) >= min_length]
        
        if not filtered_splits:
            logger.warning(f"All splits do not exceded minimum size skipping document\n")
            continue

        splits.extend(filtered_splits)
    logger.info(f"Completed Document Splits - Proceding to Embedding Stage.")    

    #embedding step
    all_vectors = []

    try:
        result = subprocess.run(["pgrep", "-f", "ollama"], capture_output=True, text=True)
        if not result.stdout.strip():
            logger.info("Starting Ollama...")
            subprocess.Popen(["ollama", "serve"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            # Wait a few seconds to ensure it starts
            time.sleep(3)
    except Exception as e:
        logger.error(f"Error starting Ollama: {e}")

    #iterate through splits
    for i, split in enumerate(splits):
        chunk_text = split.page_content
        chunk_id = str(uuid4())

        try:

            #currently failing in this block
            response = ollama.embeddings(model="nomic-embed-text", prompt = chunk_text)
            embedding = response.embedding

            if not embedding or not isinstance(embedding, list) or not all(isinstance(x, (float, int)) for x in embedding):
                logger.warning(f"Skipping chunk {i}: Invalid embedding format ({type(embedding)})")
                continue
             
            #create meta-data
            metadata = {
                "class_name":str(class_name),
                "file_type":"transcript",
                "chunk_id":str(chunk_id),
                "text": chunk_text,
            }

            if metadata:
                logger.info(f"Successfully generated chunk meta data for chunk {i}\n")

            else:
                logger.warning(f"Could not generate meta data for Chunk {i}\n")

            all_vectors.append({
                "id":chunk_id,
                "values":embedding,
                "metadata":metadata
            })
            #error embedding chunk 12-:
        except Exception as e:
                logger.error(f"Error imbedding chunk {i}\n")
                continue
        
    logger.info(f"Created {len(all_vectors)} Vectors out of {len(splits)} chunks\n")

    #attempt to upsert vectors into vector database
    if all_vectors:
            try:
                logger.info(f"Uploading {len(all_vectors)} vectors to Pinecone...")
                index.upsert(vectors=all_vectors)  # Pass the entire list at once
                logger.info("Successfully upserted document embeddings")
            except Exception as e:
                logger.error(f"Error Uploading to Pinecone: {e}")

    #remove the temporary directory after upload
    try:
        shutil.rmtree(temp_dir)
        logger.info(f"Successfully removed temporary directory: {temp_dir}")
    
    except Exception as e:
        logger.error(f"Error removing temporary directory {temp_dir}: {e}")
                
    return None






    






    





        
    

