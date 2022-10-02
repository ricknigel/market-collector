import os
from typing import TypedDict
import requests


class Attribute(TypedDict):
    projectId: str
    functionName: str
    eventTime: str


class PubSubMessage(TypedDict):
    data: list
    attributes: Attribute


webhook_url = os.getenv('SLACK_WEBHOOK_URL')


def handler(event: PubSubMessage, context):
    print(event)
    try:
        report_error(event)
    except Exception as e:
        # ここで何かしらエラーが起こった場合は、ループが起きないよう通知せず、ログに出力するのみとする。
        post_slack_message("-", "error-report", "", e)
        raise e


def report_error(event: PubSubMessage):
    error = event['data']
    projectId = event['attributes']['projectId']
    functionName = event['attributes']['functionName']
    eventTime = event['attributes']['eventTime']

    print(f"functionName: {functionName}, Time: {eventTime}, error: {error}")

    post_slack_message(projectId, functionName, eventTime, error)


def post_slack_message(projectId, functionName, eventTime, error):
    blocks = []
    requestBody = {
        "blocks": blocks
    }
    requests.post(webhook_url, json=requestBody)
