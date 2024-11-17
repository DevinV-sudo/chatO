
#imports from models and forms
from .models import ClassModel, Student
from .forms import GroupForm, UploadRosterForm, SelectClassForm, FolderUploadForm

#import tasks
from transcript import tasks

#django contrib imports
from django.contrib import messages
from django.contrib.auth import update_session_auth_hash
from django.contrib.auth.forms import PasswordChangeForm
from django.contrib.auth.models import Group, User

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
from django.http import HttpResponseNotFound



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

            # Define the HTML template content
            template_content = f"""<!DOCTYPE html>
            <html>
                <head>
                    <title>{group_name} Class</title>
                </head>
                <body>
                    <h1>Welcome to the {group_name} Class</h1>
                    <p>This is the template for the {group_name} class.</p>
                    <!-- Additional content can be added here -->
                    <form action="{{% url 'student_dashboard' %}}" method="get">
                        <button type="submit">Return to Dashboard</button>
                    </form>
                </body>
            </html>
            """

            # Define the template directory and ensure it exists
            template_directory = os.path.join('templates', 'classes')
            os.makedirs(template_directory, exist_ok=True)  # Ensure the directory exists

            # Define the full file path for the new template
            template_file_path = os.path.join(template_directory, f'{group_name}.html')

            # Write the template content to the new file
            with open(template_file_path, 'w') as template_file:  # Use open instead of default_storage
                template_file.write(template_content)

            messages.success(request, f'Template for Class {group_name} created successfully!')
    else:
        messages.info(request, f'Class {group_name} already exists.')
    return group_form
    
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
        blob_paths = []

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
                        full_blob_name = f'{class_name}/{folder_name}/{file_name}'
                        blob_paths.append(full_blob_name)
                    
                    #azure file path
                    azure_path = os.path.join(base_azure_path, folder_name, file_name)

                    # Open and read each file, then save to Azure Blob Storage
                    with open(os.path.join(root, file_name), 'rb') as file:
                        file_content = ContentFile(file.read())
                        default_storage.save(azure_path, file_content)

        #call the background task to transcribe any mp4 files
        if blob_paths:  
            tasks.process_uploaded_files(class_name, blob_paths)
        
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
    if request.method == 'POST':
        form = SelectClassForm(request.POST)
        password_form = PasswordChangeForm(request.user, request.POST)
        if password_form.is_valid():
            user = password_form.save()
            update_session_auth_hash(request, user)  # Keep the user logged in after password change
            messages.success(request, 'Your Password Has Been Changed Successfully')
            return redirect('student_dashboard')
    
        if form.is_valid():
            selected_class_id = form.cleaned_data['class_choice']
            selected_group = Group.objects.get(id = selected_class_id)
            
            try:
                student = Student.objects.get(user = request.user)
                if student.group == selected_group:
                    template_name = f'classes/{selected_group.name}.html'

                    try:
                        get_template(template_name)
                        return render(request, template_name, {'class_name': selected_group.name})
                    except TemplateDoesNotExist:
                        return HttpResponseNotFound("The class template does not exist.")
                else:
                    messages.error(request, "You are not enrolled in this class.")
            except Student.DoesNotExist:
                messages.error(request, "Student record not found.")
                return redirect('home')  # Redirect to an appropriate error handling view
    else:
        form = SelectClassForm()
        password_form = PasswordChangeForm(request.user)



    return render(request, 'groups/student_dashboard.html', {
                  'form': form,
                  'password_form' : password_form,
                  })
                  
def admin_dashboard(request):
    return render(request, 'groups/admin_dashboard.html')

def class_selection(request, class_choice):
    return render(request, 'groups/class_selection.html', {'class_choice': class_choice})

class CreateMyModelView(CreateView):
    model = ClassModel
    form_class = SelectClassForm
    success_url = reverse_lazy('student_dashboard') 
