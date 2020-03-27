import os
import uuid
import boto3
from application import application
from flask import request, send_file, abort
from globus_sdk import ConfidentialAppAuthClient
from application.pg_utils import create_table_entry, select_by_column
from application.container_handler import build_container, pull_container


@application.route('/')
def index():
    return "Hello, there!"


#TODO: Almost every function in container_handler and pg_utils logs and catches errors
# which makes it hard to tell if something failed
@application.route('/upload_def_file', methods=["POST"])
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
            definition_id = str(uuid.uuid4())
            create_table_entry("definition",
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


@application.route('/build', methods=["POST", "GET"])
def build():
    if 'Authorization' not in request.headers:
        abort(401, "You must be logged in to perform this function.")

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
                definition_entry = select_by_column("definition", definition_id=params["definition_id"])
                if definition_entry is not None and len(definition_entry) == 1:
                    definition_entry = definition_entry[0]
                    if definition_entry["definition_owner"] != client_id:
                        abort(400, "You don't have permission to use this definition file")
                    else:
                        build_id = str(uuid.uuid4())
                        build_container.apply_async(args=[client_id, params["definition_id"],
                                                          build_id, params["to_format"],
                                                          params["container_name"],
                                                          str(uuid.uuid4())])
                        return build_id
                else:
                    abort(400, "No definition DB entry for {}".format(params["definition_id"]))
            else:
                abort(400, "Missing {} parameters".format(required_params.difference(set(params.keys()))))
        elif request.method == "GET":
            build_entry = select_by_column("build", container_owner=client_id,
                                           build_id=request.json["build_id"])
            if build_entry is not None and len(build_entry) == 1:
                return build_entry[0]
            else:
                abort(400, "Build ID not valid")
    else:
        abort(400, "Failed to authenticate user")


@application.route('/pull', methods=["GET"])
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
            build_id = params["build_id"]
            build_entry = select_by_column("build", build_id=build_id)
            if build_entry is not None and len(build_entry) == 1:
                build_entry = build_entry[0]

                if build_entry["container_owner"] != client_id:
                    abort(400, "You do not have access to this definition file")
            else:
                abort(400, "Invalid build ID")

            try:
                file_name = pull_container(build_entry)
                response = send_file(os.path.basename(file_name))
                if os.path.exists(file_name):
                    os.remove(file_name)
                return response
            except Exception as e:
                file_name = os.path.join("application/",
                                         build_id + (".tar" if build_entry["container_type"] == "docker" else ".sif"))
                if os.path.exists(file_name):
                    os.remove(file_name)
                print(e)
                abort(400, "Failed to pull {}".format(build_id))
        else:
            abort(400, "No build ID")
    else:
        abort(400, "Failed to authenticate user")


