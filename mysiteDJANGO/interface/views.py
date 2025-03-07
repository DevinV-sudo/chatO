
#imports from models and forms
from .models import ClassModel, Student
from .forms import GroupForm, UploadRosterForm, SelectClassForm, FolderUploadForm

#import tasks and settings
from transcript import tasks
from django.conf import settings

#django contrib imports
from django.contrib import messages
from django.contrib.auth import update_session_auth_hash
from django.contrib.auth.forms import PasswordChangeForm
from django.contrib.auth.models import Group, User
from django.contrib.auth import update_session_auth_hash
from django.contrib.auth.decorators import login_required


#django templates imports
from django.template import TemplateDoesNotExist
from django.template.loader import get_template

#django views imports
from django.views.generic.edit import CreateView

#django urls imports
from django.urls import reverse_lazy

#django core imports
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile

#builtin imports
import os
import zipfile
import shutil
import csv

#misc. django imports
from django.shortcuts import render, redirect

#import blob client
from azure.storage.blob import BlobClient

#importing celery tasks
from transcript.tasks import (
                            process_uploaded_files,chunk_pdfs,llama_parse_batch,allocate_processing,
                            process_pdfs, whisper_transcription, upload_transcriptions,allocate_mp4_processing,
                            create_pinecone_index, markdown_chunk_embeddings, transcription_chunk_embedding,
                            audio_chunking
                            )

from celery import chain, signature
import logging

#initialize logger
logger = logging.getLogger(__name__)

def create_directories(base_path, folders):
    """Helper function to create directories."""
    try:
        for folder in folders:
            os.makedirs(folder, exist_ok=True)
    except OSError as e:
        return False, f"Failed to create directories: {e}"
    return True, "Directories created successfully"

def create_group(request):
    group_form = GroupForm(request.POST)
    if group_form.is_valid():
        group_name = group_form.cleaned_data['group_name']
        group, created = Group.objects.get_or_create(name=group_name)

        if created:
            # Create or get the class for the group
            ClassModel.objects.get_or_create(class_choice=group_name)

            # Provide success feedback to the user
            messages.success(request, f'Class {group_name} created successfully!')
        else:
            messages.info(request, f'Class {group_name} already exists.')

    else:
        messages.error(request, 'There was an error with your submission')
    return group_form

def home(request):
    logger.info(f"Request received from {request.user.username if request.user.is_authenticated else 'unauthenticated user'}")
    
    # If the user is authenticated
    if request.user.is_authenticated:
        if request.user.is_superuser:
            logger.info("Redirecting to admin dashboard")
            return redirect('admin_dashboard')  # Redirect to admin dashboard
        elif request.user.groups.filter(name='Professors').exists():
            logger.info("Redirecting to professor dashboard")
            return redirect('prof_dashboard')  # Redirect to professor dashboard
        elif request.user.groups.filter(name='Students').exists():
            logger.info("Redirecting to student dashboard")
            return redirect('student_dashboard')  # Redirect to student dashboard
        else:
            # In case the user doesn't belong to any of the specified groups
            logger.warning("User doesn't belong to any known group, redirecting to home")
            return redirect('home')  # Redirect to a default page
    else:
        # If the user is not authenticated, redirect to login
        logger.info("User is not authenticated, redirecting to login")
        return redirect('login')

def change_password(request):
    logger = logging.getLogger(__name__)
    logger.debug('Change password view hit')  # Log the hit

    if request.method == 'POST':
        password_form = PasswordChangeForm(request.user, request.POST)
        if password_form.is_valid():
            password_form.save()
            update_session_auth_hash(request, request.user)  # Keep user logged in after password change
            messages.success(request, 'Your password has been changed successfully.')
            return redirect('student_dashboard')  # Redirect back to the student dashboard
    else:
        password_form = PasswordChangeForm(request.user)
    
    return render(request, 'registration/password_change_form.html', {
        'password_form': password_form
    })
    
def upload_roster(request):
    upload_form = UploadRosterForm(request.POST, request.FILES)
    class_select_form = SelectClassForm(request.POST)  # Capture selected class

    if upload_form.is_valid() and class_select_form.is_valid():
                selected_class_id = class_select_form.cleaned_data['class_choice']
                selected_group = Group.objects.get(id=selected_class_id)
                student_group = Group.objects.get(name='Students')
                roster_file = request.FILES['roster_file']

                # Process CSV file to add students
                decoded_file = roster_file.read().decode('utf-8').splitlines()
                reader = csv.reader(decoded_file)
                for row in reader:
                    first_name, last_name = row
                    
                    # Create or get the User instance based on first and last name
                    username = f"{first_name.lower()}.{last_name.lower()}"
                    user, user_created = User.objects.get_or_create(username=username)

                    # If the user was created, set the password
                    if user_created:
                        user.set_password(username)  # Set the password to the same as username
                        user.save()

                        student_group.user_set.add(user)
                    
                    # Now create or get the Student instance
                    student, student_created = Student.objects.get_or_create(
                        user=user,
                        defaults={'first_name': first_name, 'last_name': last_name, 'group': selected_group}
                    )

                    # Update group if student already exists
                    if not student_created:
                        student.group = selected_group
                        student.save()

                # Add the user to the selected group
                selected_group.user_set.add(user)

                messages.success(request, 'Roster uploaded and Students added to class successfully!')
    return upload_form

def upload_class_data(request):
    class_data_form = FolderUploadForm(request.POST, request.FILES)
    class_select_form = SelectClassForm(request.POST)

    if class_data_form.is_valid() and class_select_form.is_valid():
        #if the form is valid pull azure info (moved from line 221)
        connection_string = settings.AZURE_CONNECTION_STRING  
        container_name = settings.AZURE_CONTAINER

        selected_class = class_select_form.cleaned_data['class_choice']  # Get class ID or name directly
        uploaded_zip = request.FILES['class_data_folder']  # Uploaded .zip file
        
        #set the folder (blob) name to the class selected
        class_name = str(Group.objects.get(id = selected_class))
        base_azure_path = f'{class_name.replace(" ", "_")}_data'
        
        # Ensure 'temp' directory exists before saving the file
        temp_dir = 'temp'
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)

        # Save the uploaded .zip file temporarily
        temp_zip_path = os.path.join(temp_dir, uploaded_zip.name)
        with open(temp_zip_path, 'wb') as temp_zip:
            for chunk in uploaded_zip.chunks():
                temp_zip.write(chunk)

        #Folder mapping, only file types specified if not in dict (misc.)
        folder_mapping = {
            ".pdf" : f'{class_name.replace(" ", "_")}_PDFs',
            ".mp4" : f'{class_name.replace(" ", "_")}_MP4s',
        }#

        #blob paths for transcriptions
        MP4_paths = []
        PDF_paths = []

        # Extract the .zip file and iterate over files
        with zipfile.ZipFile(temp_zip_path, 'r') as zip_ref:
            zip_ref.extractall('temp_unzipped')  # Extract to a temporary location

            for root, dirs, files in os.walk('temp_unzipped'):
                for file_name in files:
                    
                    #skip that annoying _MAC_OS folder
                    if '__MACOSX' in root:
                        continue
                    
                    # Determine the relative path within the folder
                    relative_path = os.path.relpath(os.path.join(root, file_name), 'temp_unzipped')

                    #Folder mapping by file type
                    file_suffix = os.path.splitext(file_name)[1].lower()
                    folder_name = folder_mapping.get(file_suffix, 'Other')
                    
                    #blob path storage for transcriptions
                    if folder_name == f'{class_name.replace(" ", "_")}_MP4s':
                        full_blob_name = f'{base_azure_path}/{folder_name}/{file_name}'
                        MP4_paths.append(full_blob_name)
                    
                    #blob path storage for PDFS
                    elif folder_name == f'{class_name.replace(" ", "_")}_PDFs':
                        full_blob_name = f'{base_azure_path}/{folder_name}/{file_name}'
                        PDF_paths.append(full_blob_name)

                    #azure file path
                    azure_path = os.path.join(base_azure_path, folder_name, file_name)

                    #Checking if a file exists before uploading
                    blob_name = f'{base_azure_path}/{folder_name}/{file_name}'
                    
                    #note moved security info out of loop

                    blob = BlobClient.from_connection_string(conn_str=connection_string,
                                                            container_name=container_name,
                                                            blob_name=blob_name )

                    #check if current file already exists, if not upload
                    if not blob.exists():

                        # Open and read each file, then save to Azure Blob Storage
                        with open(os.path.join(root, file_name), 'rb') as file:
                            file_content = ContentFile(file.read())
                            default_storage.save(azure_path, file_content)

                    else:
                        messages.error(request, f'{file_name} already exists in class data')
        
        # add empty folders: Processing, Completed, Markdown, Error
        empty_folder_names = ["Processing", "Completed", "Markdown", "Error",
                              f"{class_name.replace(' ', '_')}_PDFs",
                                f"{class_name.replace(' ', '_')}_MP4s"]
        
        #iterate through empty_folder_names
        for folder in empty_folder_names:
            blob_name = f"{base_azure_path}/{folder}/.empty"
            blob_client = BlobClient.from_connection_string(conn_str=connection_string,
                                                            container_name=container_name,
                                                            blob_name=blob_name )

            #if the intial folders exist skip, else upload the placeholders
            if not blob_client.exists():
                blob_client.upload_blob(b"",overwrite=True)
                messages.success(request, f'Created sufficient placeholder: {folder}')
            
            else:
                messages.info(request, f'{folder} already exists in class data')

        # Call the background task to transcribe any mp4 files
        # Now segmented to be more effecient
        if MP4_paths:
            blob_class = base_azure_path
            transcript_chain = chain(
                allocate_mp4_processing.s(blob_class, MP4_paths),
                process_uploaded_files.s(),
                audio_chunking.s(),
                whisper_transcription.s(),
                upload_transcriptions.s(),
                create_pinecone_index.s(),
                transcription_chunk_embedding.s()).apply_async()
            
        if PDF_paths:
            blob_class = base_azure_path
            data = (blob_class, PDF_paths)
            partition_chain = chain(
                allocate_processing.s(data),
                process_pdfs.s(),
                chunk_pdfs.s(),
                llama_parse_batch.s(),
                create_pinecone_index.s(),
                markdown_chunk_embeddings.s()).apply_async()
                
                
            
        # Clean up temporary files and folder after upload
        os.remove(temp_zip_path)
        shutil.rmtree('temp_unzipped')

        messages.success(request, 'Class data folder uploaded to Azure Blob Storage successfully!')
        
    return class_data_form, class_select_form

def prof_dashboard(request):
    # Initialize the forms and other variables before handling POST data
    group_form = GroupForm()
    upload_form = UploadRosterForm()
    class_data_form = FolderUploadForm()
    class_select_form = SelectClassForm()  # Dropdown for selecting the class
    existing_classes = ClassModel.get_class_choices()

    context = {
        'group_form': group_form,
        'upload_form': upload_form,
        'class_data_form': class_data_form,
        'class_select_form': class_select_form,
        'existing_classes': existing_classes,
    }

    if request.method == 'POST':
        if 'create_group' in request.POST:
            group_form = create_group(request)
            if group_form.is_valid():
                return redirect('prof_dashboard')
            context["group_form"] = group_form  
        
        elif 'upload_roster' in request.POST:
            upload_form = upload_roster(request)
            context["upload_form"] = upload_form  

        elif 'upload_class_data' in request.POST:
            class_data_form, class_select_form = upload_class_data(request)
            context["class_data_form"] = class_data_form
            context["class_select_form"] = class_select_form  
    
    # Render the response at the end
    return render(request, 'groups/prof_dashboard.html', context)

def student_dashboard(request):
    logger = logging.getLogger(__name__)
    logger.debug(f'Student dashboard view, method: {request.method}')

    form = SelectClassForm(request.POST or None)
    
    if request.method == 'POST':
        logger.debug('Processing form submission')

        # Handle class selection form
        if form.is_valid():
            logger.debug('Class form is valid')
            selected_class_id = form.cleaned_data['class_choice']
            try:
                selected_group = Group.objects.get(id=selected_class_id)
            except Group.DoesNotExist:
                messages.error(request, "The selected class does not exist.")
                return redirect('student_dashboard')
            
            try:
                student = Student.objects.get(user=request.user)
                if student.group == selected_group:
                    # Check if the template exists and render
                    template_name = f'classes/{selected_group.name}.html'
                    try:
                        get_template(template_name)
                        return render(request, template_name, {'class_name': selected_group.name})
                    except TemplateDoesNotExist:
                        messages.error(request, "Class template does not exist.")
                        return redirect('student_dashboard')
                else:
                    messages.error(request, "You are not enrolled in this class.")
            except Student.DoesNotExist:
                messages.error(request, "Student record not found.")
                return redirect('home')  # Redirect to an appropriate error handling view
    else:
        form = SelectClassForm()

    return render(request, 'groups/student_dashboard.html', {
        'form': form,
    })
                  
def admin_dashboard(request):
    return render(request, 'groups/admin_dashboard.html')

def class_selection(request, class_choice):
    return render(request, 'groups/class_selection.html', {'class_choice': class_choice})

class CreateMyModelView(CreateView):
    model = ClassModel
    form_class = SelectClassForm
    success_url = reverse_lazy('student_dashboard') 
