from celery import Celery
import os
import sys
from os import path
sys.path.append( path.dirname( path.dirname( path.abspath(__file__) ) ) )
app = Celery(
    'scripts',
    broker=os.environ.get('CELERY_BROKER_URL', 'pyamqp://guest@rabbit:5672//'),
    include=['intersections.tasks']
)
# todo: will need to update region for sqs .... see https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/sqs.html

if __name__ == '__main__':
    app.autodiscover_tasks(['intersections'])
