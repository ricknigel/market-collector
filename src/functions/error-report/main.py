import base64
import os
from typing import TypedDict
from zoneinfo import ZoneInfo
import requests
from datetime import datetime


class Attribute(TypedDict):
    projectId: str
    functionName: str
    eventTime: str


class PubSubMessage(TypedDict):
    data: list
    attributes: Attribute


# slackのwebhookURL
webhook_url = os.getenv("SLACK_WEBHOOK_URL")


def handler(event: PubSubMessage, context):
    """
    エンドポイント
    """
    try:
        report_error(event)
    except Exception as e:
        # ここで何かしらエラーが起こった場合は、ループが起きないよう通知せず、ログに出力するのみとする。
        now = datetime.now(ZoneInfo("Asia/Tokyo"))
        post_slack_message("-", "error-report", now, e)
        raise e


def report_error(event: PubSubMessage):
    """
    エラー内容をslackで通知します
    """
    error = base64.b64decode(event["data"]).decode("utf-8")
    projectId = event["attributes"]["projectId"]
    functionName = event["attributes"]["functionName"]
    unixtime = int(event["attributes"]["eventTime"])

    eventTime = datetime.fromtimestamp(unixtime, ZoneInfo("Asia/Tokyo"))
    print(f"functionName: {functionName}, Time: {eventTime}, error: {error}")

    # slackへ通知
    post_slack_message(projectId, functionName, eventTime, error)


def post_slack_message(
    projectId: str, functionName: str, eventTime: datetime, error: str
):
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": ":warning:  Cause Error  :warning:"
            }
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Project:*\n{projectId}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Function:*\n{functionName}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*EventTime:*\n{eventTime}"
                },
            ]
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Error Log:*\n```{error}```"
            }
        },
    ]
    requestBody = {
        "blocks": blocks
    }
    # slackへpostする
    response = requests.post(webhook_url, json=requestBody)

    if response.status_code != 200:
        error_msg = f"Slack Webhook Error \
            [HTTP STATUS: {response.status_code}], [RESULT: {response.reason}]"
        raise Exception(error_msg)
