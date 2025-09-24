from celery import Celery
import os
import sys
from os import path
from kombu.utils.url import safequote

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

app = Celery(
    'scripts',
    broker="sqs://{aws_access_key}:{aws_secret_key}@".format(
        aws_access_key=safequote(os.environ.get('CELERY_BROKER_AWS_ACCESS_KEY_ID')),
        aws_secret_key=safequote(os.environ.get(
            'CELERY_BROKER_AWS_SECRET_ACCESS_KEY'))) if os.environ.get(
        'CELERY_BROKER_AWS_ACCESS_KEY_ID') and os.environ.get(
        'CELERY_BROKER_AWS_SECRET_ACCESS_KEY') else 'pyamqp://guest@rabbit:5672//',
    broker_transport_options={'region': os.environ.get('CELERY_BROKER_TRANSPORT_OPTIONS'),
                              "is_secure": True,
                              'predefined_queues': {
                                  'celery': {
                                      'url': os.environ.get('CELERY_BROKER_QUEUE_URL'),
                                      'access_key_id': os.environ.get('CELERY_BROKER_AWS_ACCESS_KEY_ID'),
                                  }
                              }
                              } if os.environ.get(
        'CELERY_BROKER_TRANSPORT_OPTIONS') and os.environ.get('CELERY_BROKER_QUEUE_URL') else {},
    result_backend='dynamodb://{aws_access_key}:{aws_secret_key}@us-west-2/celery'.format(
        aws_access_key=safequote(os.environ.get('CELERY_BROKER_AWS_ACCESS_KEY_ID')),
        aws_secret_key=safequote(os.environ.get('CELERY_BROKER_AWS_SECRET_ACCESS_KEY'))) if os.environ.get('CELERY_BROKER_AWS_ACCESS_KEY_ID') and os.environ.get('CELERY_BROKER_AWS_SECRET_ACCESS_KEY') else 'redis://redis:6379/0',
    result_backend_transport_options={
        "is_secure": True,
    } if os.environ.get('CELERY_BROKER_AWS_ACCESS_KEY_ID') and os.environ.get('CELERY_BROKER_AWS_SECRET_ACCESS_KEY') else {},
    include=['intersections.tasks', 'sweri_utils.hosted']
)