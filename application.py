import boto3
from flask import Flask, request, send_file
from container_handler import *

app = Flask(__name__)

@app.route('/')
@app.route('/index')
def index():
    return "Hello, World!"


@app.route('/upload_def_file', methods=["POST"])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            return "No file"
        file = request.files['file']
        if file.filename == '':
            return "No file selected"
        if file:
            filename = file.filename
            conn = create_connection()
            id = str(uuid.uuid4())
            create_table_entry(conn, "container",
                               container_id=id,
                               recipe_type="docker",
                               container_name=filename,
                               container_version=1,
                               s3_location=id)
            s3 = boto3.client('s3')

            s3.upload_fileobj(file, "xtract-container-service",
                              '{}/{}'.format(id, filename))
            return id
        else:
            return "Failed"
    else:
        return "Failed"


@app.route('/build', methods=["POST"])
def build():
    params = request.json
    print(params)
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
        gen = pull_container(params["build_id"])
        for chunk in gen:
            f.write(chunk)
        response = send_file("temp.tar")
        return response
        # return send_file("./temp.tar")
    else:
        return "Failed"


if __name__ == "__main__":
    logging.basicConfig(filename='app.log',
                        filemode='w',
                        level=logging.INFO, format='%(funcName)s - %(asctime)s - %(message)s')
    app.run(debug=True)

