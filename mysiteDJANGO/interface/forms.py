from django import forms
from .models import ClassModel
from django.contrib.auth.forms import PasswordChangeForm
from django.contrib.auth.models import User




class SelectClassForm(forms.Form):
    class_choice = forms.ChoiceField(choices=ClassModel.get_class_choices(), label='Select Class')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['class_choice'].choices = ClassModel.get_class_choices()  # Update choices on initialization

class GroupForm(forms.Form):
    group_name = forms.CharField(max_length=100, label='Class Name') #changed from group name

class UploadRosterForm(forms.Form):
    roster_file = forms.FileField(label='Upload CSV Roster', required=False)#changed to false

class MultipleFileInput(forms.ClearableFileInput):
    allow_multiple_selected = True

class MultipleFileField(forms.FileField):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("widget", MultipleFileInput())
        super().__init__(*args, **kwargs)

class FolderUploadForm(forms.Form):#changed to false
    class_data_folder = forms.FileField(
        label='Upload Class Data Folder (as .zip)',
        required=False
    )
class CustomPasswordChangeForm(PasswordChangeForm):
    class Meta:
        model = User
        fields = ['old_password', 'new_password1', 'new_password2']

