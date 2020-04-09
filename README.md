# xtract-container-service
This is the repository for the Xtract Container Service (XCS), an application for pushing and pulling Docker and Singularity containers.

## Getting started for development
These instructions will get the XCS application running on your local machine for development and testing purposes.

### Prerequisites
- Docker (available [here](https://docs.docker.com/install/))
- Singularity (available [here](https://sylabs.io/guides/3.5/admin-guide/installation.html))
- AWS CLI (available [here](https://aws.amazon.com/cli/))
- AWS RDS, S3, SQS
- PostgreSQL

### Installation
1. Clone this repository and activate a virtual environment:  

        git clone https://github.com/xtracthub/xtract-container-service
        cd xtract-container-service
        virtualenv venv
        source venv/bin/activate

2. Install the requirements:

        pip install -r requirements.txt

### Setting up AWS
1. Configure your AWS CLI:

        aws configure

2. Next, create a PostgreSQL database in AWS RDS and store the following in a file named `database.ini:`

        [postgresql]
        host=YOUR_HOST_ADRESS
        database=postgres
        user=postgres
        password=YOUR_PASSWORD

3. Create an AWS S3 bucket named `xtract-container-service`

### Running XCS
1. Save your Globus Auth. Client ID and Client Secret as environment variables:

        export GL_CLIENT=YOUR_GL_CLIENT
        export GL_CLIENT_SECRET=YOUR_GL_CLIENT_SECRET

2. Save the path of the flask app. as an environment variable:
        
        export FLASK_APP=application.py

3. Start the application with root privelages:
        
        sudo flask run

4. In a second terminal, start the Celery worker:

        celery -A container_handler.celery_app worker --pool=gevent --concurrency=YOUR_MAX_THREADS

5. Ensure the Docker daemon is running using `sudo dockerd` for Ubuntu or starting Docker Desktop for Mac.


## Getting started for production
These instructions will get the XCS application running on Ubuntu for production.

### Prerequisites
- Docker (available [here](https://docs.docker.com/install/))
- Singularity (available [here](https://sylabs.io/guides/3.5/admin-guide/installation.html))
- AWS CLI (available [here](https://aws.amazon.com/cli/))
- AWS RDS, S3, SQS
- PostgreSQL
- Python3

### Installation and Apache setup
1. Install Apache.
        
        sudo apt install apache2-dev libapache2-mod-wsgi-py3

2. Clone this repository and activate a virtual environment:  

        git clone https://github.com/xtracthub/xtract-container-service
        cd xtract-container-service
        virtualenv venv
        source venv/bin/activate

3. Create a symbolic link in the `/var/www/html folder` 
        
        sudo ln -sT ~/xtrac-container-service /var/www/html/flaskapp

4. In `flaskapp.wsgi`, set your Globus Auth and AWS credentials.

5. In `etc/apache2/sites-enabled/000-default.conf`, add the following after the `DocumentRoot /var/www/html` line:
        
        WSGIDaemonProcess flaskapp threads=5 python-path=/var/www/html/flaskapp/venv/lib/python3.6/site-packages
        WSGIScriptAlias / /var/www/html/flaskapp/flaskapp.wsgi
        WSGIPassAuthorization On

        <Directory flaskapp>
            WSGIProcessGroup flaskapp
            WSGIApplicationGroup %{GLOBAL}
            Order deny,allow
            Allow from all
        </Directory>

6. Add [this](https://github.com/celery/celery/blob/master/extra/generic-init.d/celeryd) to a script  named `celeryd` in `/etc/init.d/` and make it executable:

7. Add [this](http://docs.celeryproject.org/en/latest/userguide/daemonizing.html#init-script-celeryd) to a script named `celeryd` in `/etc/default' and change the following values:
        
        CELERY_BIN="/var/www/html/flaskapp/venv/bin/celery"
        CELERY_APP="container_handler.celery_app"
        CELERYD_CHDIR="/var/www/html/flaskapp/"
        CELERYD_OPTS="--pool=gevent --concurrency=YOUR_CONCURRENCY"
        CELERYD_USER="root"
        CELERYD_GROUP="root"

8. Restart Apache.
        
        sudo service apache2 restart
        
### Running XCS

1. In one terminal, ensure that Docker is stopped and start the Docker daemon:
        
        sudo systemctl stop docker
        sudo dockerd

2. In another terminal, start the Celery daemon:
        
        sudo /etc/init.d/celeryd start
        

## Interacting with the server
XCS is a REST API so all interactions can be made with Python's request library. Examples of how to make requests can be found in `app_demo.ipynb`
(New SDK coming soon)


