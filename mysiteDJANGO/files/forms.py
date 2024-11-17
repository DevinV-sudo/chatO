# forms.py
from django import forms

class UploadFileForm(forms.Form):
    files = forms.FileField(widget=forms.ClearableFileInput())