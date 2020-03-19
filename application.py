import boto3
from flask import Flask, request, send_file, abort
from globus_sdk import ConfidentialAppAuthClient
from container_handler import *

app = Flask(__name__)


@app.route('/')
def index():
    return "Hello, World!"


@app.route('/upload_def_file', methods=["POST"])
def upload_file():
    if 'Authorization' not in request.headers:
        abort(401, 'You must be logged in to perform this function.')

    token = request.headers.get('Authorization')
    token = str.replace(str(token), 'Bearer ', '')
    conf_app = ConfidentialAppAuthClient(os.environ["GL_CLIENT"], os.environ["GL_CLIENT_SECRET"])
    intro_obj = conf_app.oauth2_token_introspect(token)

    if "client_id" in intro_obj:
        client_id = intro_obj["client_id"]

        if 'file' not in request.files:
            abort(400, "No file")
        file = request.files['file']
        if file.filename == '':
            abort(400, "No file selected")
        if file:
            filename = file.filename
            conn = create_connection()
            container_id = str(uuid.uuid4())
            create_table_entry(conn, "container",
                               container_id=container_id,
                               recipe_type="docker",
                               container_name=filename,
                               container_version=1,
                               s3_location=container_id,
                               owner=client_id)
            s3 = boto3.client('s3')

            s3.upload_fileobj(file, "xtract-container-service",
                              '{}/{}'.format(id, filename))
            return container_id
        else:
            return abort(400, "Failed to upload file")
    else:
        abort(400, "Failed to authenticate user")


@app.route('/build', methods=["POST"])
def build():
    params = request.json
    if set(params.keys()) == {"container_id", "to_format",
                              "container_name"}:
        return build_container(params["container_id"],
                               params["to_format"],
                               params["container_name"])
    else:
        return "Failed"


@app.route('/pull', methods=["GET"])
def pull():
    params = request.json
    if "build_id" in params:
        f = open('./temp.tar', 'wb')
        try:
            gen = pull_container(params["build_id"])
            for chunk in gen:
                f.write(chunk)
            response = send_file("temp.tar")
            os.remove("./temp.tar")
            return response
        except Exception as e:
            print("Exception: {}".format(e))
            os.remove("./temp.tar")
            return "Failed"
    else:
        return "Failed"


if __name__ == "__main__":
    logging.basicConfig(filename='app.log',
                        filemode='w',
                        level=logging.INFO, format='%(funcName)s - %(asctime)s - %(message)s')
    app.run(debug=True)

