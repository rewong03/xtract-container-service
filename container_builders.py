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
def build_to_singularity(container_entry, container_location):
    """Builds a Singularity container from a Dockerfile or Singularity file
    within the container db.

    Parameters:
    container_id (str): ID of container db entry to build singularity container from.
    container_location (str): Path to location to build the container.
    """
    try:
        container_id = container_entry["container_id"]
        pull_s3_dir(container_id)

        Client.load("./" + container_id)
        Client.build(build_folder=os.path.dirname(container_location),
                     image=os.path.basename(container_location))

        shutil.rmtree("./" + container_id)

        if os.path.exists(container_location):
            return container_location
        else:
            raise ValueError("Failed to build singularity container")
    except:
        return None


#TODO: Docker will require this script to be run with sudo privelages
# Possible solution: https://askubuntu.com/questions/477551/how-can-i-use-docker-without-sudo
def build_to_docker(container_entry, image_name):
    """Builds a Docker image from a container db entry.

    Parameters:
    container_id (str): ID of container db entry to build docker container from.
    image_name (str): Name to tag the final image with.

    Return:
    image (Image obj.): Docker image object.
    """
    try:
        container_id = container_entry["container_id"]
        pull_s3_dir(container_id)

        try:
            docker_client = docker.from_env()
            image = docker_client.images.build(path="./{}".format(container_id),
                                               tag=image_name)
            shutil.rmtree("./" + container_id)
        except Exception as e:
            shutil.rmtree("./" + container_id)
            raise e
        return image
    except:
        return None


def push_to_ecr(docker_image,
                ecr_link="REDACTED"):
    docker_image.tag("{}/{}:latest".format(ecr_link,
                                    docker_image.tags[0]))
    client = docker.from_env()
    # docker_image.tag(ecr_link)
    print("cool")
    # client.push("{}/{}".format(ecr_link,
    #                                 docker_image))


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


def build_container(container_id, to_format, container_name):
    """Automated pipeline for building a recipe file from the
    container db to a container.

    Parameters
    container_id (str): ID of container db entry to build from.
    to_format (str): Format of container to build. Either "singularity"
    or "docker". If "docker", the recipe type must be a Dockerfile.
    """
    try:
        assert to_format in ["docker", "singularity"], "{} is not a valid container format".format(to_format)

        container_entry = select_by_column(create_connection(),
                                           "container",
                                           container_id=container_id)

        if len(container_entry) == 1:
            container_entry = container_entry[0]
        else:
            raise ValueError("No db entry for {}".format(container_id))

        build_entry = select_by_column(create_connection(), "build",
                                       container_id=container_id,
                                       container_type=to_format)
        if len(build_entry) == 1:
            build_entry = build_entry[0]
            build_entry["build_status"] = "pending"
            update_table_entry(create_connection(), "build",
                               build_entry["build_id"], **build_entry)
        else:
            build_entry = build_schema
            build_entry["build_id"] = uuid.uuid4()
            build_entry["container_id"] = container_id
            build_entry["container_type"] = to_format
            build_entry["build_status"] = "pending"
            create_table_entry(create_connection(), "build",
                               **build_entry)

        if container_entry["recipe_type"] == "singularity" and to_format == "docker":
            update_table_entry(create_connection(), "build",
                               build_entry["build_id"], **{"build_status": "error"})
            raise ValueError("Can't build Docker container from Singularity file")

        update_table_entry(create_connection(), "build",
                           build_entry["build_id"], **{"build_status": "building"})

        if to_format == "docker":
            docker_image = build_to_docker(container_entry, container_name)
            if docker_image:
                build_time = datetime.datetime.now()
                last_built = build_entry["build_time"] if build_entry["build_time"] else None

                build_entry["build_status"] = "pushing"
                update_table_entry(create_connection(), "build",
                                   build_entry["build_id"], **{"build_status": "pushing",
                                                               "build_time": build_time,
                                                               "last_build": last_built})
                push_to_ecr(docker_image) #<- will need error catching once ecr stuff is figured out
                update_table_entry(create_connection(), "build",
                                   build_entry["build_id"], **{"build_status": "success"})
            else:
                build_entry["build_status"] = "failed"
                update_table_entry(create_connection(), "build",
                                   build_entry["build_id"], **{"build_status": "failed"})
                raise ValueError("Failed to build docker container")
        elif to_format == "singularity":
            singularity_image = build_to_singularity(container_entry, container_name)
            if singularity_image:
                build_time = datetime.datetime.now()
                last_built = build_entry["build_time"] if build_entry["build_time"] else None

                build_entry["build_status"] = "pushing"
                update_table_entry(create_connection(), "build",
                                   build_entry["build_id"], **{"build_status": "pushing",
                                                               "build_time": build_time,
                                                               "last_build": last_built})
                s3 = boto3.client("s3")
                s3.upload_fileobj(singularity_image,
                                  "xtract-container-service",
                                  "{}/{}".format(build_entry["build_id"],
                                                 container_name))
                update_table_entry(create_connection(), "build",
                                   build_entry["build_id"], **{"build_status": "success"})
            else:
                build_entry["build_status"] = "failed"
                update_table_entry(create_connection(), "build",
                                   build_entry["build_id"], **{"build_status": "failed"})
                raise ValueError("Failed to build docker container")




    except Exception as e:
        logging.error("Exception", exc_info=True)




if __name__ == "__main__":
    logging.basicConfig(filename='app.log', filemode='w',
                        level=logging.INFO, format='%(funcName)s - %(asctime)s - %(message)s')


