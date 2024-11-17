from background_task import background
from azure.storage.blob import BlobServiceClient
from django.conf import settings
from moviepy.editor import VideoFileClip
import whisper
import os
import re
import shutil

#create client
blob_service_client = BlobServiceClient.from_connection_string(settings.AZURE_CONNECTION_STRING)

#load whisper model
whisper_model = whisper.load_model('base')  # Load the model once


@background(schedule=0)
def process_uploaded_files(class_name, files):
    container_client = blob_service_client.get_container_client(settings.AZURE_CONTAINER)
    temp_download_dir = f"temp/{class_name}"
    output_paths = []
    
    try:
        os.makedirs(temp_download_dir, exist_ok=True)
        
        for blob_name in files:
            download_path = os.path.join(temp_download_dir, os.path.basename(blob_name))
            blob_client = container_client.get_blob_client(blob_name)
            
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
                output_paths.append(output_audio_path)
            except Exception as e:
                print(f"Error processing video file {download_path}: {e}")
            finally:
                os.remove(download_path)
    except Exception as e:
        print(f"Error processing uploaded files for {class_name}: {e}")
        raise

    whisper_transcription(class_name=class_name, mp3_files=output_paths)


@background(schedule=0)
def whisper_transcription(class_name, mp3_files):
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
            print(f"Error transcribing audio file {audio_path}: {e}")
    
    # Proceed to upload transcriptions
    upload_transcriptions(class_name, transcript_files)

@background(schedule=0)
def upload_transcriptions(class_name, transcript_files):
    try:
        container_client = blob_service_client.get_container_client(settings.CONTAINER_NAME)
        
        for transcript_file in transcript_files:
            try:
                blob_name = f"{class_name}_transcripts/{os.path.basename(transcript_file)}"
                blob_client = container_client.get_blob_client(blob_name)

                with open(transcript_file, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)

                os.remove(transcript_file)  # Clean up local file after upload
            except Exception as e:
                print(f"Error uploading {transcript_file}: {e}")

        print(f"Successfully uploaded {len(transcript_files)} transcripts for {class_name}")
    except Exception as e:
        print(f"Error uploading transcripts for {class_name}: {e}")
        raise

