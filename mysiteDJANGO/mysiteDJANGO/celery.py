from __future__ import absolute_import, unicode_literals
import os
from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mysiteDJANGO.settings')

app = Celery('mysiteDJANGO')

app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()


