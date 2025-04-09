from celery import Celery
import os

app = Celery(
    'scripts',
    broker=os.environ.get('CELERY_BROKER_URL', 'pyamqp://guest@localhost:5672//')
)
# todo: will need to update region for sqs .... see https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/sqs.html

if __name__ == '__main__':
    app.autodiscover_tasks(['intersections'], force=True)
    # app.autodiscover_tasks(['intersections'])
