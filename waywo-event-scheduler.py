from datetime import datetime
import json
import time
import os
import random

import boto3

MAX_TIME_SECONDS = int(os.environ["MAX_TIME_SECONDS"])
MIN_SECONDS_BETWEEN_EVENTS = int(
    os.environ.get("MIN_SECONDS_BETWEEN_EVENTS", 0))
EVENT_NAMESPACE = os.environ["EVENT_NAMESPACE"]
CLIENT = boto3.client('dynamodb')

def lambda_handler(event, context):
    now = datetime(
        *time.strptime(event["time"], "%Y-%m-%dT%H:%M:%SZ")[:6]
    ).timestamp()

    upper_bound = now + MAX_TIME_SECONDS
    lower_bound = now - MIN_SECONDS_BETWEEN_EVENTS

    print("The time is now {}".format(now))

    print("Checking for events already scheduled within the time bound...")

    response = CLIENT.query(
        TableName = 'scheduled-events',
        KeyConditionExpression = \
            "EventNamespace = :namespace AND EventTime BETWEEN :lower_bound AND :upper_bound",
        FilterExpression = "EventExecutedAt = :null",
        ExpressionAttributeValues = {
            ":namespace": {
                "S": EVENT_NAMESPACE
            },
            ":lower_bound": {
                "N": str(lower_bound)
            },
            ":upper_bound": {
                    "N": str(upper_bound)
            },
            ":null": {
                "NULL": True
            }
        }
    )

    print("DyanmobDB response: {}".format(response))

    most_recent_event_time = max(
        int(event["EventTime"]["N"])
        for event in response["Items"]) if response["Items"] else None

    if (most_recent_event_time and (most_recent_event_time >= now)):
        print(
            "Event already scheduled at {}. "
            "This is within {} seconds. Exiting".format(
                most_recent_event_time,
                MAX_TIME_SECONDS))

        return {
            'statusCode': 200,
            'body': json.dumps({
                "namespace": EVENT_NAMESPACE,
                "event_scheduled": False,
                "event_max_offset": MAX_TIME_SECONDS,
                "min_seconds_between_events": MIN_SECONDS_BETWEEN_EVENTS,
            })
        }

    lower_bound_event_time = max(
        now,
        most_recent_event_time + MIN_SECONDS_BETWEEN_EVENTS) \
        if most_recent_event_time else now

    upper_bound_event_time = now + MAX_TIME_SECONDS

    print("Choosing an event time between {} and {}".format(
        lower_bound_event_time, upper_bound_event_time))

    schedule_event_at = random.randrange(
        lower_bound_event_time,
        upper_bound_event_time)

    print("Scheduling event at {}".format(schedule_event_at))

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
                "N": str(now),
            },
            "EventMaxOffset": {
              "N": str(MAX_TIME_SECONDS),
            },
            "EventExecutedAt": {
                "NULL": True
            }
        }
    )

    print("DynamoDB response: {}".format(scheduled_event))

    return {
        'statusCode': 200,
        'body': json.dumps({
            "namespace": EVENT_NAMESPACE,
            "event_time": schedule_event_at,
            "event_scheduled": True,
            "event_scheduled_at": now,
            "event_max_offset": MAX_TIME_SECONDS,
            "min_seconds_between_events": MIN_SECONDS_BETWEEN_EVENTS,
            "event_time_lower_bound": lower_bound_event_time,
            "event_time_upper_bound": upper_bound_event_time,
        })
    }