from celery import Celery
import os
import sys
from os import path
sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )
app = Celery(
    'scripts',
    broker=os.environ.get('CELERY_BROKER_URL', 'pyamqp://guest@rabbit:5672//'),
    broker_transport_options={'region': os.environ.get('CELERY_BROKER_TRANSPORT_OPTIONS')} if os.environ.get('CELERY_BROKER_TRANSPORT_OPTIONS') else {},
    result_backend=os.environ.get('CELERY_RESULT_BACKEND') if os.environ.get('CELERY_RESULT_BACKEND') else 'redis://redis:6379/0',
    include=['intersections.tasks']
)
# todo: will need to update region for sqs .... see https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/sqs.html


