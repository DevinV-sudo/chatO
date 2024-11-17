from django.contrib import admin
from django.urls import path, include
from . import views

app_name = 'files'

urlpatterns = [
    path("upload/", views.upload_file, name = 'upload_file'),
    
]