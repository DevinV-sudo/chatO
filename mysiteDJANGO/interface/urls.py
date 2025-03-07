# interface/urls.py
from django.urls import path
from . import views
from django.contrib.auth import views as auth_views
from django.urls import path, include
from django.contrib.auth.decorators import login_required
from .views import change_password , student_dashboard
from django.contrib.auth.views import LoginView


urlpatterns = [
    path('admin/dashboard/', views.admin_dashboard, name='admin_dashboard'),
    path('prof/dashboard/', views.prof_dashboard, name='prof_dashboard'),
    path('student/dashboard/', student_dashboard, name='student_dashboard'),
    path('password_change/', change_password, name='password_change_page'),
    path('password_change/done/', auth_views.PasswordChangeDoneView.as_view(template_name='registration/password_change_done.html'), name='password_change_done'),
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),
    path('', views.home, name='home'),
    path('login/', LoginView.as_view(), name='login'),
]

