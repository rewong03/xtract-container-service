import json
import boto3


def get_message(queue_name="xtract-container-service"):
    """Receives a message from an SQS queue.

    Parameters:
    queue_name (str): Name of queue to pull off of.

    Returns:
    message (dict): Dict. of received message.
    """
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    response = queue.receive_messages(MaxNumberOfMessages=1)

    if len(response) == 1:
        response = response[0]
        message = json.loads(response.body)
        response.delete()

        return message
    else:
        return None


def put_message(message, queue_name="xtract-container-service"):
    """Places a message on an SQS queue.

    Parameters:
    message (dict): Message to pass to SQS.
    queue_name (str): SQS to put message on.

    Returns:
    response (dict): Response from SQS
    """
    message = json.dumps(message)
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    response = queue.send_message(MessageBody=message)

    return response
