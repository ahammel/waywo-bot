from datetime import datetime
import json
import os
import time

import boto3

CHECK_LAST_SECONDS = int(os.environ["CHECK_LAST_SECONDS"])
EVENT_NAMESPACE = os.environ["EVENT_NAMESPACE"]

DYNAMODB = boto3.client('dynamodb')
SNS = boto3.client('sns')

TOPIC_ARN = "arn:aws:sns:us-west-2:985967100294:waywo-sns-topic"

def lambda_handler(event, context):
    event_time = datetime(
        *time.strptime(event["time"], "%Y-%m-%dT%H:%M:%SZ")[:6]
    ).timestamp()

    lower_bound = event_time - CHECK_LAST_SECONDS
    print("The time is now {}".format(event_time))
    print("Checking for new events since {}".format(lower_bound))

    response = DYNAMODB.query(
        TableName = 'scheduled-events',
        KeyConditionExpression = \
            "EventNamespace = :namespace AND EventTime BETWEEN :lower_bound AND :now",
        FilterExpression = "EventExecutedAt = :null",
        ExpressionAttributeValues = {
            ":namespace": {
                "S": EVENT_NAMESPACE
            },
            ":lower_bound": {
                "N": str(lower_bound)
            },
            ":now": {
                    "N": str(event_time)
            },
            ":null": {
                "NULL": True
            }
        }
    )

    print("DynamoDB response: {}".format(response))

    items = response["Items"]

    if (len(items) == 0):
        return {
        'statusCode': 200,
        'body': json.dumps({
            "events_executed": False
        })
    }

    for event in items:
        response = DYNAMODB.update_item(
            TableName = "scheduled-events",
            Key = {
                "EventNamespace": event["EventNamespace"],
                "EventTime": event["EventTime"]
            },
            UpdateExpression = "SET EventExecutedAt = :event_time",
            ExpressionAttributeValues = {
                ":event_time": { "N": str(event_time) },
            }
        )
        print("Update event response: {}".format(response))

    notification = {
        "events": [
            {
                "event_namespace": event["EventNamespace"]["S"],
                "event_time": event["EventTime"]["N"],
                "event_executed_at": event_time,
            }
            for event in items
        ]
    }

    SNS.publish(
        TopicArn = TOPIC_ARN,
        Message = json.dumps(notification)
    )

    return  {
        'statusCode': 200,
        'body': json.dumps({
            "events_executed": True,
            "notification": notification,
        })
    }

