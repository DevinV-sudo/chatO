# views.py
from django.urls import reverse_lazy
from django.views.generic import CreateView
from django.contrib.auth.models import Group
from django.shortcuts import redirect
from .forms import CustomUserCreationForm

class SignUpView(CreateView):
    form_class = CustomUserCreationForm  # Use the custom form
    success_url = reverse_lazy("login")
    template_name = "registration/signup.html"

    def form_valid(self, form):
        # Save the user first
        user = form.save()

        # Get the user type from the form data
        user_type = form.cleaned_data.get('user_type')

        # Add the user to the corresponding group
        if user_type == 'student':
            group = Group.objects.get(name='Students')
        elif user_type == 'professor':
            group = Group.objects.get(name='Professors')
        
        user.groups.add(group)

        # Return to the success URL (login page)
        return redirect(self.success_url)