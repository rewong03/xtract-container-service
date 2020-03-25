# xtract-container-service
This is the repository for the Xtract Container Service (XCS), an application for pushing and pulling Docker and Singularity containers.

## Getting Started
THese instructinos will get the XCS application running on your local machine for development and testing purposes.

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

### Setup
First, configure your AWS CLI:
```
aws configure
```
Next, create a file named database.ini and store your AWS RDS info:
```
[postgresql]
host=YOUR_HOST_ADRESS
database=postgres
user=postgres
password=YOUR_PASSWORD
```
Then, create a AWS S3 bucket and SQS queue named `xtract-container-service`

### Running XCS
First, save your Globus Auth. Client ID and Client Secret as environment variables:
```
export GL_CLIENT=YOUR_GL_CLIENT
export GL_CLIENT_SECRET=YOUR_GL_CLIENT_SECRET
```
Then, start the application:
```
python3 application.py
```

## Interacting with the server
XCS is a REST API so all interactions can be made with Python's request library. Examples of how to make requests can be found in `app_demp.ipynb`


