from django.db import models
from django.contrib.auth.models import Group, User  # Importing Group model to associate with classes


class Student(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)  # Ensure this is correct
    first_name = models.CharField(max_length=30)
    last_name = models.CharField(max_length=30)
    group = models.ForeignKey('auth.Group', on_delete=models.CASCADE, related_name='students')  # Links to the Group model

    def __str__(self):
        return f'{self.first_name} {self.last_name}'

class ClassModel(models.Model):
    class_choice = models.CharField(max_length=100, unique=True)

    def __str__(self):
        return self.class_choice

    @classmethod
    def get_class_choices(cls):
        # Fetch groups from the Django admin
        groups_with_no_permissions = [
            (group.id, group.name) for group in Group.objects.all() if not group.permissions.exists()
        ]
        return groups_with_no_permissions