import urllib
import botocore.session
from flask import Flask
from celery import Celery


app = Flask(__name__)
aws_credentials = botocore.session.get_session().get_credentials()
celery_app = Celery(app.name, broker="sqs://{}:{}@".format(urllib.parse.quote(aws_credentials.access_key, safe=''),
                                                           urllib.parse.quote(aws_credentials.secret_key, safe='')))

from app import routes