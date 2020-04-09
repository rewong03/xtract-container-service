activate_this = '/var/www/html/flaskapp/venv/bin/activate_this.py'
with open(activate_this) as file_:
    exec(file_.read(), dict(__file__=activate_this))

import os
import sys
sys.path.insert(0, "/var/www/html/flaskapp")

os.environ["GL_CLIENT"] = "YOUR_GL_CLIENT"
os.environ["GL_CLIENT_SECRET"] = "YOUR_GL_CLIENT_SECRET"
os.environ["AWS_ACCESS_KEY_ID"] = "YOUR_AWS_ACCESS_KEY_ID"
os.environ["AWS_SECRET_ACCESS_KEY"] = "YOUR_AWS_SECRET_ACCESS_KEY"

from application import application