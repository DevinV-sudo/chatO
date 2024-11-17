from django.shortcuts import render
from .forms import UploadFileForm

def upload_file(request):
    if request.method == "POST":
        form = UploadFileForm(request.POST, request.FILES)
        file = request.FILES['file']
        return render(request, 'dataload/success.html', {'file': file})
    else:
        form = UploadFileForm()
    return render(request, 'dataload/upload.html', {'form': form})