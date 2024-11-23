#import celery module
from celery import shared_task

#import azure tools
from azure.storage.blob import BlobServiceClient
from django.conf import settings

#video-audio-text packages
from moviepy.editor import VideoFileClip
import whisper

#other misc. imports
import os
import re
import shutil
import logging

#pdf packages
import fitz

#logging for celery tasks
logger = logging.getLogger(__name__)

#create client
blob_service_client = BlobServiceClient.from_connection_string(settings.AZURE_CONNECTION_STRING)

#load whisper model
whisper_model = whisper.load_model('base')  # Load the model once

#import numpy
import numpy as np


'''
Take in pdf paths at begining, pass out transcript paths, pdf paths, class name to partition links, make one chain
'''
@shared_task(acks_late=True, bind=True)
def process_uploaded_files(self, class_name, MP4_files, PDF_files):  # renamed parameter to avoid confusion
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
    data = (class_name, processed_mp3_files, PDF_files)  # return the processed files
    logger.info(f"PROCESS FILES END - Returning data: {data}")

    return data


@shared_task(acks_late=True, bind=True)
def whisper_transcription(self, data):
    logger.info(f"WHISPER START - Received data type: {type(data)}")
    logger.info(f"WHISPER START - Raw data: {data}")
    
    class_name, mp3_files, PDF_files = data
    temp_transcript_dir = f"temp/{class_name}/transcripts"
    os.makedirs(temp_transcript_dir, exist_ok=True)
    
    transcript_files = []

    for audio_path in mp3_files:
        try:
            transcription_name = os.path.splitext(os.path.basename(audio_path))[0]
            transcription_file = os.path.join(temp_transcript_dir, f"{transcription_name}_transcription.txt")

            # Transcribe and write to file
            transcription_text = whisper_model.transcribe(audio_path, fp16=False)["text"].strip()
            with open(transcription_file, "w") as f:
                f.write(transcription_text)

            transcript_files.append(transcription_file)
        except Exception as e:
            logger.info(f"Error transcribing audio file {audio_path}: {e}")
    
    # Proceed to upload transcriptions
    logger.info("Uploading transcriptions to Azure blob storage")
    data = (class_name, transcript_files, PDF_files)
    return data
    
@shared_task(acks_late=True, bind=True)
def upload_transcriptions(self, data):
    class_name, transcript_files, PDF_files = data
    temp_download_dir = f'temp/{class_name}'
    transcript_paths = []

    try:
        container_client = blob_service_client.get_container_client(settings.AZURE_CONTAINER)
        
        for transcript_file in transcript_files:
            try:
                blob_name = f"{class_name}_transcripts/{os.path.basename(transcript_file)}"
                blob_client = container_client.get_blob_client(blob_name)

                with open(transcript_file, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                    transcript_paths.append(blob_name)

                os.remove(transcript_file)  # Clean up local file after upload
            except Exception as e:
                logger.info(f"Error uploading {transcript_file}: {e}")

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
    
    #callling partition now
    logger.info("preparing documents to partition")
    data = class_name, transcript_paths, PDF_files
    return data
    


def num_pages(blob_path):
    page_count = 0
    try:
        container_client = blob_service_client.get_container_client(settings.AZURE_CONTAINER)
        blob_client = container_client.get_blob_client(blob_path)

        # Get blob properties
        blob_properties = blob_client.get_blob_properties()
        blob_size = blob_properties.size  # Size in bytes

        # Calculate pages
        size_in_100kb_units = blob_size / (100 * 1024)  # Convert to 100KB units
        pages = int(size_in_100kb_units) + 1  # Add 1 to account for partial pages
        
        logger.info(f"Successfully calculated page count for blob '{blob_path}': {pages} pages")
        return pages

    except Exception as e:
        logger.error(f"Failed to calculate page count for blob '{blob_path}': {e}")
        return 0



@shared_task(ack_late=True, bind=True)
def documents_to_partition(self, data):
    #unpacking data
    logger.info("Determining documents to partition")
    class_name, transcript_paths, PDF_files = data
    
    #combining the paths for processing
    data_paths = transcript_paths + PDF_files

    #the azure container with the blobs for pdfs
    container_client = blob_service_client.get_container_client(settings.AZURE_CONTAINER)

    #storage for documents < 1000 pages
    partition_bucket = []
    queue = []
    
    page_count = 0

    #iterate through each blob and determine if it is image heavy
    temp_part_dir = f"temp/{class_name}_partitions"
    os.makedirs(temp_part_dir, exist_ok=True)

    try:
        for blob in data_paths:
            number_of_pages = num_pages(blob)
            download_path = os.path.join(temp_part_dir, os.path.basename(blob))
            blob_client = container_client.get_blob_client(blob)

            try:
                with open(download_path, "wb") as download_file:
                    download_file.write(blob_client.download_blob().readall())
                
                if page_count + number_of_pages < 1000:
                    partition_bucket.append(download_path)
                    page_count += number_of_pages
                    logger.info(f"Added {blob} to partition bucket, page count = {page_count}")
                else:
                    if not queue:
                        logger.info(f"Starting queue with {blob}")
                    queue.append(download_path)
                    page_count = number_of_pages

            except Exception as e:
                logger.error(f"Failed to process {blob}: {e}")
                continue 
    
        logger.info(f"PDFS for {class_name} have been processed")

    except Exception as e:
        logger.error(f"An error occurred proccessing pdfs: {e}")
        raise
    
    partition_docs = (partition_bucket, queue)
    logger.info("Uploading partition documents to Azure Blob Storage")
    data = (class_name, partition_docs, temp_part_dir)
    return data       

#WORKING ON THIS NEED TO FINISH TODAY
@shared_task(acks_late=True, bind=True)
def upload_partitions(self, data):
    class_name, paritition_docs, temp_part_dir = data
    partition_bucket, queue = paritition_docs
    temp_download_dir = temp_part_dir

    logger.info("UPLOADING PARTITIONS")
    try:
        container_client = blob_service_client.get_container_client(settings.AZURE_CONTAINER)
        
        for document in partition_bucket:
            try:
                blob_name = f"{class_name}_partition_bucket/{os.path.basename(document)}"
                blob_client = container_client.get_blob_client(blob_name)

                with open(document, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)

                os.remove(document)  # Clean up local file after upload
            except Exception as e:
                logger.info(f"Error uploading {document}: {e}")

        logger.info(f"Successfully uploaded {len(partition_bucket)} partitions for {class_name}")

        for document in queue:
            try:
                blob_name = f"{class_name}_partition_queue/{os.path.basename(document)}"
                blob_client = container_client.get_blob_client(blob_name)

                with open(document, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                os.remove(document)  # Clean up local file after upload
            
            except Exception as e:
                logger.info(f"Error uploading {document}: {e}")
        logger.info(f"Successfully uploaded {len(partition_bucket)} partitions for {class_name}")

    except Exception as e:
        logger.info(f"Error uploading partitions for {class_name}: {e}")
        raise
    
    try:
        shutil.rmtree(temp_part_dir)
    except OSError as e:
        logger.warning(f"Failed to delete {temp_part_dir}: {e}", exc_info=True)




