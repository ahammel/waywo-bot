from datetime import datetime
import json
import time
import os
import random

import boto3

MAX_TIME_SECONDS = int(os.environ["MAX_TIME_SECONDS"])
EVENT_NAMESPACE = os.environ["EVENT_NAMESPACE"]
CLIENT = boto3.client('dynamodb')

def lambda_handler(event, context):
    event_time = datetime(
        *time.strptime(event["time"], "%Y-%m-%dT%H:%M:%SZ")[:6]
    ).timestamp()

    print("The time is now {}".format(event_time))
    print("The event will be scheduled no sooner than {}".format(
        MAX_TIME_SECONDS))

    schedule_event_at = event_time + (random.randrange(MAX_TIME_SECONDS))

    scheduled_event = CLIENT.put_item(
        TableName = "scheduled-events",
        Item = {
            "EventNamespace": {
                "S": EVENT_NAMESPACE,
            },
            "EventTime": {
                "N": str(schedule_event_at),
            },
            "EventScheduledAt": {
                "N": str(event_time),
            },
            "EventMaxOffset": {
              "N": str(MAX_TIME_SECONDS),
            },
            "EventExecutedAt": {
                "NULL": True
            }
        }
    )

    print("Event scheduled at {}".format(schedule_event_at))

    return {
        'statusCode': 200,
        'body': json.dumps({
            "namespace": EVENT_NAMESPACE,
            "event_time": schedule_event_at,
            "event_scheduled_at": event_time,
            "event_max_offset": MAX_TIME_SECONDS
        })
    }
