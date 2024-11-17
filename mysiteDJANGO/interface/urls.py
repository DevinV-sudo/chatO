# interface/urls.py
from django.urls import path
from . import views
from django.contrib.auth import views as auth_views
from django.urls import path, include


urlpatterns = [
    path('admin/dashboard/', views.admin_dashboard, name='admin_dashboard'),
    path('prof/dashboard/', views.prof_dashboard, name='prof_dashboard'),
    path('student/dashboard/', views.student_dashboard, name='student_dashboard'),
    path('class/select/', views.CreateMyModelView.as_view(), name='class_select'),  # URL for form submission
    path('class/<str:class_choice>/', views.class_selection, name='class_selection'),  # Define the new URL
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),

]

