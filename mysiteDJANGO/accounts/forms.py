from django import forms
from django.contrib.auth.forms import UserCreationForm
from django.contrib.auth.models import User

class CustomUserCreationForm(UserCreationForm):
    USER_TYPE_CHOICES = [
        ('student', 'Student'),
        ('professor', 'Professor'),
    ]
    user_type = forms.ChoiceField(choices=USER_TYPE_CHOICES, label='Select User Type')

    class Meta:
        model = User
        fields = ('username', 'email', 'password1', 'password2', 'user_type')