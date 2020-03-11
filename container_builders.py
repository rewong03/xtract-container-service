import datetime
import os
import logging
import subprocess
import shutil
import uuid
import boto3
import docker
import namegenerator
from spython.main import Client
from spython.main.parse.parsers import get_parser
from spython.main.parse.writers import get_writer
from pg_utils import *


def pull_s3_dir(container_id):
    """Pulls a directory of files from a container_id folder in our
    S3 bucket.

    Parameter:
    container_id (str): Name of id to pull files from.
    """
    bucket = boto3.resource('s3').Bucket("xtract-container-service")
    for object in bucket.objects.filter(Prefix=container_id):
        if not os.path.exists("./" + os.path.dirname(object.key)):
            os.makedirs("./" + os.path.dirname(object.key))
        bucket.download_file(object.key, "./" + object.key)


#TODO: Temporary fix for catching when singularity fails to build a definition file
# Usually singularity just prints the fail statement thorugh the singularity application
# instead of through python
def build_to_singularity(container_id, container_location):
    """Builds a Singularity container from a Dockerfile or Singularity file
    within the container db.

    Parameters:
    container_id (str): ID of container db entry to build singularity container from.
    container_location (str): Path to location to build the container.
    """
    try:
        assert len(select_by_column(create_connection(), "container",
                                           container_id=container_id)) > 0, "Recipe doesn't exist"

        pull_s3_dir(container_id)
        Client.load("./" + container_id)
        build_entry = select_by_column(create_connection(), "build",
                                       container_id=container_id,
                                       container_type="singularity")
        if len(build_entry) > 0:
            build_entry = build_entry[0]
            build_entry["build_status"] = "building"
            update_table_entry(create_connection(), "build",
                               build_entry["build_id"], **build_entry)
        else:
            build_entry = build_schema
            build_entry["build_id"] = uuid.uuid4()
            build_entry["container_id"] = container_id
            build_entry["container_type"] = "singularity"
            build_entry["build_location"] = container_location
            build_entry["build_status"] = "building"
            create_table_entry(create_connection(), "build",
                               **build_entry)

        Client.build(build_folder=os.path.dirname(container_location),
                     image=os.path.basename(container_location))

        if os.path.exists(container_location):
            build_time = datetime.datetime.now()

            if build_entry["creation_time"]:
                build_entry["last_built"] = build_entry["creation_time"]
            else:
                build_entry["last_built"] = build_time
            build_entry["build_status"] = "successful"
            build_entry["creation_time"] = build_time
            build_entry["container_name"] = os.path.basename(container_location)
            build_entry["container_size"] = os.path.getsize(container_location)

            update_table_entry(create_connection(), "build",
                               build_entry["build_id"], **build_entry)
            shutil.rmtree("./" + container_id)
            logging.info("Successfully built %s Singularity container at %s",
                         os.path.basename(container_id), container_location)

        else:
            build_entry["build_status"] = "failed"
            update_table_entry(create_connection(), "build",
                               build_entry["build_id"], **build_entry)
            shutil.rmtree("./" + container_id)
            raise ValueError("Singularity failed to build {}".format(os.path.basename(container_id)))
    except Exception as e:
        logging.error("Exception", exc_info=True)


#TODO: Docker will require this script to be run with sudo privelages
# Possible solution: https://askubuntu.com/questions/477551/how-can-i-use-docker-without-sudo
def build_to_docker(container_id, image_name):
    """Builds a Docker image from a container db entry.

    Parameters:
    container_id (str): ID of container db entry to build docker container from.
    image_name (str): Name to tag the final image with.
    """
    try:
        assert len(select_by_column(create_connection(), "container",
                                    container_id=container_id,
                                    recipe_type="docker")) > 0, "Recipe doesn't exit"

        pull_s3_dir(container_id)
        docker_client = docker.from_env()
        build_entry = select_by_column(create_connection(), "build",
                                       container_id=container_id,
                                       container_type="docker")
        if len(build_entry) > 0:
            build_entry = build_entry[0]
            build_entry["build_status"] = "building"
            update_table_entry(create_connection(), "build",
                               build_entry["build_id"], **build_entry)
        else:
            build_entry = build_schema
            build_entry["build_id"] = uuid.uuid4()
            build_entry["container_id"] = container_id
            build_entry["container_type"] = "docker"
            build_entry["build_location"] = "daemon"
            build_entry["build_status"] = "building"
            create_table_entry(create_connection(), "build",
                               **build_entry)

        try:
            docker_client.images.build(path="./{}".format(container_id),
                                       tag=image_name)
            build_time = datetime.datetime.now()

            if build_entry["creation_time"]:
                build_entry["last_built"] = build_entry["creation_time"]
            else:
                build_entry["last_built"] = build_time
            build_entry["build_status"] = "successful"
            build_entry["creation_time"] = build_time
            build_entry["container_name"] = image_name
            for image in docker_client.df()["Images"]:
                if image_name in image["RepoTags"][0]:
                    build_entry["container_size"] = image["Size"]
                    break

            update_table_entry(create_connection(), "build",
                               build_entry["build_id"], **build_entry)
        except Exception as e:
            build_entry["build_status"] = "failed"
            update_table_entry(create_connection(), "build",
                               build_entry["build_id"], **build_entry)

            raise e
        finally:
            shutil.rmtree("./" + container_id)

        logging.info("Successfully built %s Docker image", image_name)
    except Exception as e:
        logging.error("Exception", exc_info=True)


#TODO: Find a better way to name converted Singularity definition files
def convert_definition_file(container_id, singularity_def_name=None):
    """Converts a Dockerfile to a Singularity definition file or vice versa.

    Parameters:
    container_id (str): ID of container db entry to convert.
    singularity_def_name (str): Name to give to converted .def file if converting
    from Dockerfile to Singularity definition file.
    """
    try:
        container_entry = select_by_column(create_connection(), "container",
                                           container_id=container_id)

        if len(container_entry) > 0:
            container_entry = container_entry[0]
        else:
            raise ValueError("Recipe doesn't exist")

        new_container_id = uuid.uuid4()
        new_path = "./" + str(new_container_id)
        os.mkdir(new_path)
        #TODO: S3's file structure is flat rather than nested so it makes it hard to download files using
        # python but in the future we should download directories using boto3
        subprocess.call("aws s3 cp --recursive s3://xtract-container-service/{} {}".format(container_id,
                                                                                           new_path),
                        shell=True)

        for file in os.listdir(new_path):
            if file == "Dockerfile" or file.endswith(".def"):
                input_file = file
                break
            else:
                input_file = None

        assert input_file, "Definition file not found"

        if input_file.endswith(".def"):
            from_format = "Singularity"
            to_format = "docker"
        else:
            from_format = "docker"
            to_format = "Singularity"

        file_parser = get_parser(from_format)
        file_writer = get_writer(to_format)
        parser = file_parser(os.path.join(new_path, input_file))
        writer = file_writer(parser.recipe)
        result = writer.convert()

        if to_format == "Singularity":
            if singularity_def_name is None:
                singularity_def_name = namegenerator.gen() + ".sif"

            file_path = os.path.join(str(new_container_id), singularity_def_name)
        else:
            file_path = os.path.join(str(new_container_id), "Dockerfile")

        with open(file_path, 'w') as f:
            f.write(result)

        os.remove(os.path.join(new_path, input_file))

        db_entry = container_schema
        db_entry["container_id"] = new_container_id
        db_entry["recipe_type"] = to_format.lower()
        db_entry["container_name"] = container_entry["container_name"]
        db_entry["container_version"] = 1
        db_entry["pre_containers"] = container_entry["pre_containers"]
        db_entry["post_containers"] = container_entry["post_containers"]
        db_entry["replaces_container"] = container_entry["replaces_container"]
        db_entry["s3_location"] = str(new_container_id)
        create_table_entry(create_connection(), "container",
                           **db_entry)


        logging.info("Successfully converted %s %s definition file to %s %s definition file",
                     os.path.basename(input_file),
                     from_format,
                     os.path.basename(file_path),
                     to_format)

    except Exception as e:
        logging.error("Exception", exc_info=True)
    finally:
        shutil.rmtree(new_path)


if __name__ == "__main__":
    logging.basicConfig(filename='app.log', filemode='w',
                        level=logging.INFO, format='%(funcName)s - %(asctime)s - %(message)s')
    # build_to_singularity("662ef5c1-2de7-4071-89c2-552cad75985e", "./test.sif")
    convert_definition_file("13729cf8-a205-416c-87fc-2421e97a84b9")
    # pull_s3_dir("662ef5c1-2de7-4071-89c2-552cad75985e")
    # prep_database("test.db")
    # db = "test.db"
    # build_to_singularity("test.def", "blah/my_test.sif")
    # build_to_docker("d782ffbf-84de-447f-91ce-b29c6142ba76", "bad-image")
    # build_singularity_from_docker('/Users/ryan/Documents/CS/CDAC/singularity-vm/xtract-container-service/Dockerfile', './tabby.sif')
    # convert_definition_file("blah/Dockerfile", "blah")

