import json
import os
import tempfile
import uuid
import boto3
from flask import abort, Flask, request, send_file
from globus_sdk import ConfidentialAppAuthClient
from container_handler import convert_definition_file, pull_container
from pg_utils import build_schema, create_table_entry, prep_database, select_by_column, table_exists, update_table_entry
from sqs_queue_utils import put_message
from task_manager import TaskManager


app = Flask(__name__)
manager = TaskManager(max_threads=11, kill_time=30)


@app.route("/")
def process_requests():
    print(request.json)
    return "working"