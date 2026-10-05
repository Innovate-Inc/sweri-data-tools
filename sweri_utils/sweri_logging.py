import logging
import sys
import json
from urllib.parse import urlencode
import os
from slack_sdk import WebClient
import requests
from datetime import datetime

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', encoding='utf-8',
                    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S', force=True)
# try:
#     import watchtower
#     logger.addHandler(watchtower.CloudWatchLogHandler())
# except Exception as e:
#     logger.warning(f'watchtower not available: {e}')

stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)
def log_this(func):
    def wrapper(*args, **kwargs):
        logger.info(f"Starting: {func.__name__} with {args} {kwargs}")
        result = func(*args, **kwargs)
        logger.info(f"Finished: {func.__name__}")
        return result
    return wrapper


class ProcessingStatusLogger:
    def __init__(self, process: str = '', steps: dict | None = None):
        self.feature_service_url = os.getenv('PROCESSING_STATUS_FEATURE_SERVICE_URL', '')
        self.esri_token=os.getenv('PORTAL_API_KEY', '')
        self.slack_channel_id = os.getenv('SLACK_CHANNEL_ID', '')
        self.slack_token = os.getenv('SLACK_TOKEN', '')
        self.environment = os.getenv('ENVIRONMENT', '')
        self.steps = steps if steps is not None else {}
        self.status = 'Starting'
        self.start = datetime.now()
        self.environment = self.environment
        self.process = process

        if not self.slack_channel_id or not self.slack_token:
            logger.info("Slack channel ID or token is not provided. Skipping Slack logging.")
            self.slack_client = None
            self.slack_channel_id = None
        else:
            self.slack_channel_id = self.slack_channel_id
            self.slack_client = WebClient(token=self.slack_token)  # Replace with your Slack bot token
            self.slack_message_id = None

        if not self.feature_service_url or not self.esri_token:
            logger.info("Feature service URL or ESRI token is not provided. Skipping feature service logging.")
            self.feature_service_url = None
            self.esri_token = None
        else:
            self.feature_service_url = self.feature_service_url
            self.esri_token = self.esri_token
            self.feature_globalid = None
            self.feature_objectid = None

        self.stop = None
        self.log_status()

    def send_message(self, channel, message):
        try:
            response = self.slack_client.chat_postMessage(channel=channel, text=message)
            return response
        except Exception as e:
            logger.info(f"Error sending message to Slack: {e}")
            return None

    def format_esri_payload(self):
        feature = {"attributes": {
            "status": self.status,
            "details": json.dumps(self.steps),
            "start": self.start.timestamp()*1000,  # Convert to milliseconds
            "environment": self.environment,
            "process": self.process
        }}
        if self.stop:
            feature["attributes"]["stop"] = self.stop.timestamp()*1000  # Convert to milliseconds
        if self.feature_globalid:
            feature["attributes"]["globalid"] = self.feature_globalid
        if self.feature_objectid:
            feature["attributes"]["objectid"] = self.feature_objectid

        edit_type = "adds" if self.feature_globalid is None else "updates"
        return {edit_type: json.dumps([feature])}

    def log_status_to_feature_service(self):
        if not self.feature_service_url or not self.esri_token:
            return

        token = self.esri_token  # Get the token from your hosted service
        headers = {
            "Authorization": f"Bearer {token}",
        }
        payload = self.format_esri_payload()
        try:
            response = requests.post(f"{self.feature_service_url}/applyEdits", data=payload, headers=headers, params={"f": "json"})
            response.raise_for_status()
            if 'error' in response.json():
                raise Exception(response.json()['error'])

            if self.feature_globalid is None:
                self.feature_globalid = response.json()['addResults'][0]["globalId"]
            if self.feature_objectid is None:
                self.feature_objectid = response.json()['addResults'][0]["objectId"]
        except Exception as e:
            logger.info(f"Error logging status to feature service: {e}")

    def log_status(self):
        self.log_status_to_feature_service()
        self.log_status_to_slack()

    def format_slack_message(self):
        if self.status == "Completed":
            status_emoji = ":white_check_mark:"
        elif self.status == "Failed":
            status_emoji = ":x:"
        elif self.status == "Starting":
            status_emoji = ":hourglass_flowing_sand:"
        else:
            status_emoji = ":running:"

        return [
            {
                "type": "markdown",
                "text": (
                    f"# {self.environment.capitalize()} {self.process}\n"
                    f"**Status**: {self.status} {status_emoji}\n"
                    f"| Step | Status |\n|---|---|\n"
                    f"| Started | {self.start.isoformat()} |\n"
                    f"{'\n'.join([f'| {step.replace('_', ' ').capitalize()} | {status} |' for step, status in self.steps.items()])}\n"
                    f"| Stopped | {self.stop.isoformat() if self.stop else ''} |"
                )
            }
        ]

    def log_status_to_slack(self):
        if not self.slack_channel_id or not self.slack_client:
            return

        message = self.format_slack_message()
        try:
            if self.slack_message_id is None:
                response = self.slack_client.chat_postMessage(
                    channel=self.slack_channel_id,
                    blocks=message
                )
                if response and response.get("ok"):
                    self.slack_message_id = response.get("ts")
            else:
                self.slack_client.chat_update(
                    channel=self.slack_channel_id,
                    blocks=message,
                    ts=self.slack_message_id
                )
        except Exception as e:
            logger.info(f"Error logging status to Slack: {e}")


    def update_step(self, step_key, step_status):
        self.status = 'Running'
        if step_key in self.steps:
            self.steps[step_key] = step_status
            self.log_status()
        else:
            logger.info(f"Step key '{step_key}' not found in steps.")

    def complete(self):
        self.status = 'Completed'
        self.stop = datetime.now()
        self.log_status()

    def fail(self):
        self.status = 'Failed'
        self.stop = datetime.now()
        self.log_status()
