from celery import Celery
import os
import sys
from os import path
from kombu.utils.url import safequote

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

app = Celery(
    'scripts',
    broker="sqs://{aws_access_key}:{aws_secret_key}@".format(aws_access_key=safequote(os.environ.get(
        'CELERY_BROKER_AWS_ACCESS_KEY_ID')),
        aws_secret_key=safequote(os.environ.get(
            'CELERY_BROKER_AWS_SECRET_ACCESS_KEY')) )if os.environ.get(
            'CELERY_BROKER_AWS_ACCESS_KEY_ID') and os.environ.get(
            'CELERY_BROKER_AWS_SECRET_ACCESS_KEY') else 'pyamqp://guest@rabbit:5672//',
        broker_transport_options={'region': os.environ.get('CELERY_BROKER_TRANSPORT_OPTIONS')} if os.environ.get(
            'CELERY_BROKER_TRANSPORT_OPTIONS') else {},
        result_backend=os.environ.get('CELERY_RESULT_BACKEND') if os.environ.get(
            'CELERY_RESULT_BACKEND') else 'redis://redis:6379/0',
        include=['intersections.tasks']
    )
# todo: will need to update region for sqs .... see https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/sqs.html
