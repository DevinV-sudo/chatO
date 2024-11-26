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
import time

#import numpy
import numpy as np

#import pinecone
from pinecone import Pinecone, ServerlessSpec

#import unstructured requirements
from unstructured_ingest.v2.pipeline.pipeline import Pipeline
from unstructured_ingest.v2.interfaces import ProcessorConfig
from unstructured_ingest.v2.processes.partitioner import PartitionerConfig
from unstructured_ingest.v2.processes.connectors.fsspec.azure import (
    AzureIndexerConfig,
    AzureDownloaderConfig,
    AzureConnectionConfig,
    AzureAccessConfig
)
from unstructured_ingest.v2.processes.connectors.pinecone import (PineconeConnectionConfig, PineconeAccessConfig, PineconeUploaderConfig, PineconeUploadStagerConfig)
from unstructured_ingest.v2.processes.chunker import ChunkerConfig
from unstructured_ingest.v2.processes.embedder import EmbedderConfig

#logging for celery tasks
logger = logging.getLogger(__name__)

#create client
blob_service_client = BlobServiceClient.from_connection_string(settings.AZURE_CONNECTION_STRING)

#load whisper model
whisper_model = whisper.load_model('base')  # Load the model once

#pinecone auth services
pinecone_api_key = settings.PINECONE_API_KEY
pc = Pinecone(api_key=pinecone_api_key)

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
    return class_name

@shared_task(ack_late=True, bind=True)
def create_pinecone_index(self, class_name):
    logger.info(f"[{self.request.id}] Creating Pinecone index for class: {class_name}")
    index_name = class_name.lower().replace("_", "-").replace(" ", "-").strip()
    try:
        existing_indexes = [index_info["name"] for index_info in pc.list_indexes()]
        logger.debug(f"Existing indexes: {existing_indexes}")

        if index_name not in existing_indexes:
            logger.debug(f"Creating new index {index_name} with dimension 3072")
            pc.create_index(
                name=index_name,
                dimension=3072,
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
            data = (class_name, index_name)
            return data
        else:
            logger.info(f"[{self.request.id}] Index already exists: {index_name}")
            index = pc.Index(index_name)
            stats = index.describe_index_stats()
            logger.debug(f"Existing index stats: {stats}")
            data = (class_name, index_name)
            return data

    except Exception as e:
        logger.error(f"[{self.request.id}] Error creating Pinecone index: {e}", exc_info=True)
        raise

@shared_task(ack_late=True, bind=True)
def unstructured_pipeline(self, data):
    class_name, index_name = data
    
    #logging pipeline initialization
    logger.info(f"Processing data from {class_name}, Uploading to Pinecone Index: {index_name}\n")
    
    #set up temporary directories
    partition_directory = f"temp/{class_name}_unstructured/"
    os.makedirs(partition_directory, exist_ok=True)

    try:
        #beginning pipeline
        logger.info(f"[{self.request.id}] Starting partition_documents task for class: {class_name}")
        
        #reporting intial index stats
        index = pc.Index(index_name)
        initial_stats = index.describe_index_stats()
        logger.info(f"Initial index stats: {initial_stats}")
        
        #pipeline
        Pipeline.from_configs(
            context=ProcessorConfig(),
            indexer_config=AzureIndexerConfig(remote_url=f"az://django-container/{class_name}_partition_bucket/"),
            downloader_config=AzureDownloaderConfig(download_dir = partition_directory),
            source_connection_config=AzureConnectionConfig(
                access_config=AzureAccessConfig(
                    account_name=settings.AZURE_ACCOUNT_NAME,
                    sas_token=settings.AZURE_SAS_TOKEN,
                )
            ),
            partitioner_config=PartitionerConfig(
                partition_by_api=True,
                api_key=settings.UNSTRUCTURED_API_KEY,
                partition_endpoint=settings.UNSTRUCTURED_URL,
                strategy="fast",
                additional_partition_args={
                    "split_pdf_page": True,
                    "split_pdf_allow_failed": True,
                    "split_pdf_concurrency_level": 15,
                    "extract_image_block_types": [],
                },
            ),
            chunker_config=ChunkerConfig(
                chunk_by_api = True,
                chunk_api_key=settings.UNSTRUCTURED_API_KEY,
                chunking_strategy="by_similarity",
                chunk_similarity_threshold=0.75,  # Try increasing this
                chunk_max_characters=1000,  # Try increasing this
                chunk_new_after_n_characters=750,  # Adjust this                
                chunk_include_original_elements=True,
                chunkmultipage_sections=True,
                chunk_overlap=100,  # Try increasing overlap
            ),
            embedder_config=EmbedderConfig(
                embedding_provider="openai",
                embedding_model_name="text-embedding-3-large",
                embedding_api_key=settings.OPENAI_API_KEY,
            ),
            destination_connection_config=PineconeConnectionConfig(
                access_config=PineconeAccessConfig(
                    api_key=settings.PINECONE_API_KEY
                ),
                index_name= index_name
            ),
            
            stager_config=PineconeUploadStagerConfig(),
            uploader_config=PineconeUploaderConfig()
        ).run()
        
        #report completion
        logger.info(f"[{self.request.id}] Partitioning completed for class: {class_name}")

        # Verify upload success
        logger.info("Pipeline execution completed. Verifying results...")
        final_stats = index.describe_index_stats()
        logger.info(f"Final index stats: {final_stats}")
        
        if final_stats['total_vector_count'] == initial_stats['total_vector_count']:
            logger.warning("No new vectors were added to the index")
            logger.debug(f"Initial count: {initial_stats['total_vector_count']}")
            logger.debug(f"Final count: {final_stats['total_vector_count']}")
        else:
            vectors_added = final_stats['total_vector_count'] - initial_stats['total_vector_count']
            logger.info(f"Successfully added {vectors_added} vectors to the index")
            
        logger.info(f"[{self.request.id}] Partitioning completed for class: {class_name}")
    
    except Exception as e:
        logger.error(f"[{self.request.id}] Error partitioning documents: {e}", exc_info=True)
        raise
    finally:
        # Clean up temp directory
        if os.path.exists(partition_directory):
            shutil.rmtree(partition_directory)
        logger.info(f"Cleaned up temporary directory: {partition_directory}")



        
    

