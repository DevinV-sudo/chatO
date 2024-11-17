from django.shortcuts import redirect
from django.urls import resolve
from django.utils.deprecation import MiddlewareMixin
from . import views

class GroupRedirectMiddleware(MiddlewareMixin):
    def process_view(self, request, view_func, view_args, view_kwargs):
        user = request.user

        if request.path.startswith('/admin/'):
            return None
        
        # Get the name of the current view based on the URL
        current_url_name = resolve(request.path_info).url_name
        
        # List of URLs to exclude from redirection
        excluded_urls = ['login', 'signup', 'logout', 'admin']
        
        if current_url_name in excluded_urls:
            return None

        if user.is_authenticated:
            # Check if the user is in the Admin group and not already on the admin page
            if user.groups.filter(name='Admin').exists() and current_url_name != 'admin_dashboard':
                return redirect('admin_dashboard')
            # Check if the user is in the Professors group and not already on their dashboard
            elif user.groups.filter(name='Professors').exists() and current_url_name != 'prof_dashboard':
                return redirect('prof_dashboard')
            # Check if the user is in the Students group and not already on their dashboard
            elif user.groups.filter(name='Students').exists() and current_url_name != 'student_dashboard':
                return redirect('student_dashboard')
            # If already on the correct page, don't redirect
            else:
                return None  # Already on the correct page, do nothing

        # Allow all other requests to pass
        return None
    
    #Upload folder, write instructions above upload - display upload, save into local folder named after class, with sub folders of data type
    #figure out group / class creation and access
    #figure out chatbox to write llm responses too
    