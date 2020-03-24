import threading
import boto3
from flask import Flask, request, send_file, abort
from globus_sdk import ConfidentialAppAuthClient
from container_handler import *
from sqs_queue_utils import *

app = Flask(__name__)


@app.after_request
def submit_tasks(response):
    MAX_THREADS = 8 #Just an arbitrary number for now
    while True:
        print("______")
        task = pull_off_queue()
        print("NUMBER OF THREADS: {}".format(threading.active_count()))
        print("TASK DETAILS: {}".format(task))
        if threading.active_count() >= MAX_THREADS or task is None:
            break
        else:
            print("Trying to start thread...")
            thread = threading.Thread(target=build_container, args=tuple(task.values()))
            thread.start()
            print("Thread started...")
            time.sleep(3)
    print("Done")
    return response


@app.route('/')
def index():
    return "Hello, World!"


#TODO: Almost every function in container_handler and pg_utils logs and catches errors
# which makes it hard to tell if something failed
@app.route('/upload_def_file', methods=["POST"])
def upload_file():
    if 'Authorization' not in request.headers:
        abort(401, 'You must be logged in to perform this function.')

    token = request.headers.get('Authorization')
    token = str.replace(str(token), 'Bearer ', '')
    conf_app = ConfidentialAppAuthClient(os.environ["GL_CLIENT"], os.environ["GL_CLIENT_SECRET"])
    intro_obj = conf_app.oauth2_token_introspect(token)

    if "client_id" in intro_obj:
        client_id = str(intro_obj["client_id"])

        if 'file' not in request.files:
            abort(400, "No file")
        file = request.files['file']
        if file.filename == '':
            abort(400, "No file selected")
        if file:
            filename = file.filename
            conn = create_connection()
            definition_id = str(uuid.uuid4())
            create_table_entry(conn, "definition",
                               definition_id=definition_id,
                               definition_type="docker" if filename == "Dockerfile" else "singularity",
                               definition_name=filename,
                               s3_location=definition_id,
                               definition_owner=client_id)
            s3 = boto3.client('s3')

            s3.upload_fileobj(file, "xtract-container-service",
                              '{}/{}'.format(definition_id, filename))
            return definition_id
        else:
            return abort(400, "Failed to upload file")
    else:
        abort(400, "Failed to authenticate user")


@app.route('/build', methods=["POST", "GET"])
def build():
    if 'Authorization' not in request.headers:
        abort(401, 'You must be logged in to perform this function.')

    token = request.headers.get('Authorization')
    token = str.replace(str(token), 'Bearer ', '')
    conf_app = ConfidentialAppAuthClient(os.environ["GL_CLIENT"], os.environ["GL_CLIENT_SECRET"])
    intro_obj = conf_app.oauth2_token_introspect(token)

    if "client_id" in intro_obj:
        client_id = str(intro_obj["client_id"])
        if request.method == "POST":
            params = request.json
            required_params = {"definition_id", "to_format", "container_name"}
            if set(params.keys()) >= required_params:
                build_id = str(uuid.uuid4())
                task = {"owner_id": client_id, "definition_id": params["definition_id"],
                        "build_id": build_id, "to_format": params["to_format"],
                        "container_name": params["container_name"]}
                put_on_queue(task)
                return build_id
            else:
                abort(400, "Missing {} parameters".format(required_params.difference(set(params.keys()))))
        elif request.method == "GET":
            build_entry = select_by_column(create_connection(), "build",
                                           container_owner=client_id,
                                           build_id=request.json["build_id"])
            if build_entry is not None and len(build_entry) == 1:
                return build_entry[0]
            else:
                abort(400, "Build ID not valid")
    else:
        abort(400, "Failed to authenticate user")


@app.route('/pull', methods=["GET"])
def pull():
    if 'Authorization' not in request.headers:
        abort(401, 'You must be logged in to perform this function.')

    token = request.headers.get('Authorization')
    token = str.replace(str(token), 'Bearer ', '')
    conf_app = ConfidentialAppAuthClient(os.environ["GL_CLIENT"], os.environ["GL_CLIENT_SECRET"])
    intro_obj = conf_app.oauth2_token_introspect(token)

    if "client_id" in intro_obj:
        client_id = intro_obj["client_id"]
        params = request.json
        if "build_id" in params:
            try:
                file_path = pull_container(client_id, params["build_id"])
                response = send_file(file_path)
                if os.path.exists(file_path):
                    os.remove(file_path)

                return response
            except Exception as e:
                if os.path.exists(file_path):
                    os.remove(file_path)
                print("Exception: {}".format(e))
                return "Failed"
        else:
            return "Failed"
    else:
        abort(400, "Failed to authenticate user")


if __name__ == "__main__":
    logging.basicConfig(filename='app.log',
                        filemode='w',
                        level=logging.INFO, format='%(funcName)s - %(asctime)s - %(message)s')
    app.run(debug=True, threaded=True)

