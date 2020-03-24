import json
import boto3

sqs = boto3.resource('sqs')
sqs2 = boto3.client('sqs')


def put_on_queue(msg):
    """Sends a message to the xtract-container-service queue.

    Parameter:
    msg (dict): Dictionary to send to queue.

    Return:
    response (str): Response from queue.
    """
    queue = sqs.get_queue_by_name(QueueName='xtract-container-service')

    str_msg = json.dumps(msg)

    response = queue.send_message(MessageBody=str_msg)
    return response


def pull_off_queue():
    response = sqs2.receive_message(
    QueueUrl='https://sqs.us-east-2.amazonaws.com/576668000072/xtract-container-service',
    AttributeNames=[
        'SentTimestamp'
    ],
    MaxNumberOfMessages=1,
    MessageAttributeNames=[
        'All'
    ],
    VisibilityTimeout=0,
    WaitTimeSeconds=0)

    if "Messages" in response:
        message = response["Messages"][0]
        body = message["Body"]
        receipt_handle = message['ReceiptHandle']
        sqs2.delete_message(
            QueueUrl='https://sqs.us-east-2.amazonaws.com/576668000072/xtract-container-service',
            ReceiptHandle=receipt_handle)
        return json.loads(body)

    else:
        print("Messages unavailable!")
        return None
