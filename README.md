# xtract-container-service
This is the repository for the Xtract Container Service (XCS), an application for pushing and pulling Docker and Singularity containers.

## Getting Started
These instructinos will get the XCS application running on your local machine for development and testing purposes.

### Prerequisites
- Docker (available [here](https://docs.docker.com/install/))
- Singularity (available [here](https://sylabs.io/guides/3.5/admin-guide/installation.html))
- AWS CLI (available [here](https://aws.amazon.com/cli/))
- AWS RDS, S3, SQS

### Installation
First, clone this repository and activate a virtual environment:
```
git clone https://github.com/xtracthub/xtract-container-service
cd xtract-container-service
python3 -m venv venv
source venv/bin/activate
```
Next, install the requirements:
```
pip install -r requirements.txt
```
### Setting up AWS
First, configure your AWS CLI:
```
aws configure
```
Next, create a PostgreSQL database in AWS RDS and store the following in a file named `database.ini:`
```
[postgresql]
host=YOUR_HOST_ADRESS
database=postgres
user=postgres
password=YOUR_PASSWORD
```
Finally, create an AWS S3 bucket named `xtract-container-service`

### Running XCS
First, save your Globus Auth. Client ID and Client Secret as environment variables:
```
export GL_CLIENT=YOUR_GL_CLIENT
export GL_CLIENT_SECRET=YOUR_GL_CLIENT_SECRET
```
Next, save the path of the flask app. as an environment variable:
```
export FLASK_APP=application.py
```
Then, start the application:
```
flask run
```
Finally, in a second terminal, start the Celery worker:
```
celery -A container_handler.celery_app worker --pool=gevent --concurrency=YOUR_MAX_THREADS
```

## Interacting with the server
XCS is a REST API so all interactions can be made with Python's request library. Examples of how to make requests can be found in `app_demp.ipynb`


