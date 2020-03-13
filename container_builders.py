import base64
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
        #TODO Find a better way to error check
        if os.path.exists(container_location):
            logging.info("Successfully built {}".format(container_location))
            return container_location
        else:
            raise ValueError("Failed to build singularity container")
    except:
        return None


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

        logging.info("Successfully build {}".format(image_name))
        return image
    except:
        return None


def push_to_ecr(docker_image, build_id, image_name):
    """Pushes a docker image to an ECR repository.

    Parameters:
    docker_image (Docker image obj.): Docker image object to push.
    build_id (str): Build UUID of docker_image.
    image_name (str): Name of the image of docker_image.

    Return:
    (str): Response message from docker push or None if it fails.
    """
    try:
        ecr_client = boto3.client("ecr")
        try:
            ecr_client.describe_repositories(repositoryNames=[build_id])
        except:
            ecr_client.create_repository(repositoryName=build_id)
        token = ecr_client.get_authorization_token()
        username, password = base64.b64decode(token['authorizationData'][0]['authorizationToken']).decode().split(':')
        registry = token['authorizationData'][0]['proxyEndpoint'] + "/" + build_id
        docker_client = docker.from_env()
        docker_client.login(username=username, password=password,
                            registry=registry)
        docker_image.tag(registry[8:],
                         tag=image_name)
        #TODO Sometimes there are errors where the docker login is successful but
        response = docker_client.images.push(registry[8:])
        # TODO Find a better way to check if the image was successfully pushed
        if "sha256" in response:
            return response
        else:
            raise ValueError("Failed to push")
    except:
        return None


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
    container_name (str): Name to give the container or path for path for
    Singularity container.
    """
    try:
        assert to_format in ["docker", "singularity"], "{} is not a valid container format".format(to_format)

        container_entry = select_by_column(create_connection(),
                                           "container",
                                           container_id=container_id)

        if container_entry is not None and len(container_entry) == 1:
            container_entry = container_entry[0]
        else:
            raise ValueError("No db entry for {}".format(container_id))

        build_entry = select_by_column(create_connection(), "build",
                                       container_id=container_id,
                                       container_type=to_format)
        if build_entry is not None and len(build_entry) == 1:
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
                docker_image = docker_image[0]
                last_built = build_entry["build_time"] if build_entry["build_time"] else None
                build_time = datetime.datetime.now()

                for image in docker.from_env().df()["Images"]:
                    if container_name in image["RepoTags"][0]:
                        container_size = image["Size"]

                update_table_entry(create_connection(), "build",
                                   build_entry["build_id"], **{"build_status": "pushing",
                                                               "build_time": build_time,
                                                               "last_built": last_built,
                                                               "container_size": container_size})
                response = push_to_ecr(docker_image, str(build_entry["build_id"]),
                                       container_name)
                if response is not None:
                    update_table_entry(create_connection(), "build",
                                       build_entry["build_id"], **{"build_status": "success"})
                    docker.from_env().images.remove(container_name, force=True)
                else:
                    update_table_entry(create_connection(), "build",
                                       build_entry["build_id"], **{"build_status": "failed"})
                    docker.from_env().images.remove(container_name, force=True)
                    raise ValueError("Failed to push")
            else:
                update_table_entry(create_connection(), "build",
                                   build_entry["build_id"], **{"build_status": "failed"})
                raise ValueError("Failed to build docker container")

        elif to_format == "singularity":
            if container_name.endswith(".sif"):
                singularity_image = build_to_singularity(container_entry, container_name)
            else:
                update_table_entry(create_connection(), "build",
                                   build_entry["build_id"], **{"build_status": "failed"})
                raise ValueError("Invalid Singularity container name")
            if singularity_image:
                build_time = datetime.datetime.now()
                last_built = build_entry["build_time"] if build_entry["build_time"] else None
                image_size = os.path.getsize(container_name)

                build_entry["build_status"] = "pushing"
                update_table_entry(create_connection(), "build",
                                   build_entry["build_id"], **{"build_status": "pushing",
                                                               "build_time": build_time,
                                                               "last_built": last_built,
                                                               "container_size": image_size})
                s3 = boto3.client("s3")
                s3.upload_fileobj(open(singularity_image, 'rb'),
                                  "xtract-container-service",
                                  "{}/{}".format(build_entry["build_id"],
                                                 os.path.basename(container_name)))
                update_table_entry(create_connection(), "build",
                                   build_entry["build_id"], **{"build_status": "success"})
                os.remove(container_name)
            else:
                build_entry["build_status"] = "failed"
                update_table_entry(create_connection(), "build",
                                   build_entry["build_id"], **{"build_status": "failed"})
                raise ValueError("Failed to build singularity container")

    except Exception as e:
        logging.error("Exception", exc_info=True)



if __name__ == "__main__":
    logging.basicConfig(filename='app.log', filemode='w',
                        level=logging.INFO, format='%(funcName)s - %(asctime)s - %(message)s')
    #TODO s3 is pushing an empty folder to the s3 bucket for the singularity build
    # also lets automatically delete singularity containers after pushing
    build_container("cd660770-b993-4058-8f82-a350f1f0dedb", "singularity", "./lmao.sif")
