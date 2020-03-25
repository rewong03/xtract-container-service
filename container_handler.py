import datetime
import os
import logging
import subprocess
import shutil
import tempfile
import time
import uuid
import boto3
import docker
import namegenerator
from spython.main import Client
from spython.main.parse.parsers import get_parser
from spython.main.parse.writers import get_writer
from pg_utils import *


def pull_s3_dir(definition_id):
    """Pulls a directory of files from a definition_id folder in our
    S3 bucket.

    Parameter:
    definition_id (str): Name of id to pull files from.
    """
    try:
        bucket = boto3.resource('s3').Bucket("xtract-container-service")
        for object in bucket.objects.filter(Prefix=definition_id):
            if not os.path.exists("./" + os.path.dirname(object.key)):
                os.makedirs("./" + os.path.dirname(object.key))
            bucket.download_file(object.key, "./" + object.key)
    except Exception as e:
        print(e)
        raise e


def ecr_login():
    """Logs Docker into ECR registry.

    Return:
    registry (str): Name of the ECR registry logged into.
    """
    ecr_client = boto3.client('ecr')
    token = ecr_client.get_authorization_token()
    registry = token['authorizationData'][0]['proxyEndpoint']
    subprocess.call(
        "aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin {}".format(registry),
        shell=True)

    return registry


def push_to_ecr(docker_image, build_id, image_name):
    """Pushes a docker image to an ECR repository.

    Parameters:
    docker_image (Docker image obj.): Docker image object to push.
    build_id (str): Build UUID of docker_image.
    image_name (str): Name of the image of docker_image.

    Return:
    (str): ID of pushed Docker image or None if the push fails.
    """
    try:
        ecr_client = boto3.client("ecr")
        try:
            ecr_client.describe_repositories(repositoryNames=[build_id])
        except:
            ecr_client.create_repository(repositoryName=build_id)
        registry = ecr_login()[8:] + "/" + build_id
        docker_client = docker.from_env()
        docker_image.tag(registry,
                         tag=image_name)
        response = docker_client.images.push(registry, stream=False)
        # TODO Find a better way to check if the image was successfully pushed
        if "sha256" in response:
            return docker_image.id[7:]
        else:
            print("FAILED: {}".format(response))
            raise ValueError("Failed to push")
    except Exception as e:
        print("EXCEPTION: {}".format(e))
        return None


def pull_container(build_id):
    """Pulls Docker containers from ECR and Singularity containers from S3.

    Parameters:
    build_id (str): ID of container to pull.

    Return:
    (file obj. gen.): File generator for the Docker container.
    """
    try:
        build_entry = select_by_column(create_connection(), "build",
                                       build_id=build_id)
        if build_entry is not None and len(build_entry) == 1:
            build_entry = build_entry[0]
        else:
            raise ValueError("Invalid build ID")

        if build_entry["container_type"] == "docker":
            registry = ecr_login()
            docker_client = docker.from_env()
            image = docker_client.images.pull(registry[8:], tag=build_entry["container_name"])
            return image.save(chunk_size=10485760)
        else:
            return "pass"

    except Exception as e:
        print(e)
        logging.error("Exception", exc_info=True)


def build_to_singularity(definition_entry, container_location):
    """Builds a Singularity container from a Dockerfile0 or Singularity file
    within the definition db.

    Parameters:
    definition_entry (str): Entry of definition db entry to build singularity container from.
    container_location (str): Path to location to build the container.

    Return:
    container_location: Returns the location of the Singularity container or None if it
    fails to save.
    """
    try:
        definition_id = definition_entry["definition_id"]
        pull_s3_dir(definition_id)
        Client.load("./" + definition_id)
        #TODO: It looks like the rm and forcerm kwargs aren't doing what they're supposed to
        # so we can't fully delete images after being built
        Client.build(image=os.path.join("./", container_location),
                     rm=True, forcerm=True)
        shutil.rmtree("./" + definition_id)
        #TODO Find a better way to error check
        if os.path.exists(container_location):
            logging.info("Successfully built {}".format(container_location))
            return container_location
        else:
            raise ValueError("Failed to build singularity container")
    except Exception as e:
        print(e)
        return None


def build_to_docker(definition_entry, image_name):
    """Builds a Docker image from a definition db entry.

    Parameters:
    definition_entry (str): Entry of definition db entry to build docker container from.
    image_name (str): Name to tag the final image with.

    Return:
    image (Image obj.): Docker image object or None if the container fails to build.
    """
    try:
        definition_id = definition_entry["definition_id"]
        pull_s3_dir(definition_id)
        try:
            docker_client = docker.from_env()
            image = docker_client.images.build(path="./{}".format(definition_id),
                                               tag=image_name)
            shutil.rmtree("./" + definition_id)
        except Exception as e:
            print(e)
            shutil.rmtree("./" + definition_id)
            logging.error("Exception", exc_info=True)
            raise e

        logging.info("Successfully build {}".format(image_name))
        return image
    except Exception as e:
        print(e)
        return None


#TODO: Find a better way to name converted Singularity definition files
def convert_definition_file(definition_id, singularity_def_name=None):
    """Converts a Dockerfile0 to a Singularity definition file or vice versa.

    Parameters:
    definition_id (str): ID of definitio db entry to convert.
    singularity_def_name (str): Name to give to converted .def file if converting
    from Dockerfile0 to Singularity definition file.
    """
    try:
        definition_entry = select_by_column(create_connection(), "definition",
                                            definition_id=definition_id)

        if len(definition_entry) > 0:
            definition_entry = definition_entry[0]
        else:
            raise ValueError("Recipe doesn't exist")

        new_definition_id = str(uuid.uuid4())
        new_path = "./" + str(new_definition_id)
        os.mkdir(new_path)
        #TODO: S3's file structure is flat rather than nested so it makes it hard to download files using
        # python but in the future we should download directories using boto3
        subprocess.call("aws s3 cp --recursive s3://xtract-container-service/{} {}".format(definition_id,
                                                                                           new_path),
                        shell=True)

        for file in os.listdir(new_path):
            if file == "Dockerfile0" or file.endswith(".def"):
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

            file_path = os.path.join(str(new_definition_id), singularity_def_name)
        else:
            file_path = os.path.join(str(new_definition_id), "Dockerfile0")

        with open(file_path, 'w') as f:
            f.write(result)

        os.remove(os.path.join(new_path, input_file))

        db_entry = definition_schema
        db_entry["definition_id"] = new_definition_id
        db_entry["recipe_type"] = to_format.lower()
        db_entry["definition_name"] = definition_entry["container_name"]
        db_entry["container_version"] = 1
        db_entry["pre_containers"] = definition_entry["pre_containers"]
        db_entry["post_containers"] = definition_entry["post_containers"]
        db_entry["replaces_container"] = definition_entry["replaces_container"]
        db_entry["s3_location"] = str(new_definition_id)
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


def build_container(owner_id, definition_id, build_id, to_format, container_name):
    """Automated pipeline for building a recipe file from the
    definition db to a container.

    Parameters:
    owner_id (str): ID of definition file owner as returned by Globus Auth.
    token introspection.
    definition_id (str): ID of definition db entry to build from.
    to_format (str): Format of container to build. Either "singularity"
    or "docker". If "docker", the recipe type must be a Dockerfile0.
    container_name (str): Name to give the container or path for path for
    Singularity container.
    build_id (str): UUID to give to a new build entry. build_id should be None if
    a build entry exists for definition_id.

    Return:
    build_id (str): Build id of the built container or failed if the container
    failed to build.
    """
    try:
        assert to_format in ["docker", "singularity"], "{} is not a valid container format".format(to_format)
        definition_entry = select_by_column(create_connection(),
                                            "definition",
                                            definition_id=definition_id)
        if definition_entry is not None and len(definition_entry) == 1:
            definition_entry = definition_entry[0]

            if definition_entry["definition_owner"] != owner_id:
                return "You do not have access to this definition file"
        else:
            raise ValueError("No db entry for {}".format(definition_id))
        build_entry = select_by_column(create_connection(), "build",
                                       definition_id=definition_id,
                                       container_type=to_format)
        if build_entry is not None and len(build_entry) == 1:
            build_entry = build_entry[0]
            build_entry["build_status"] = "pending"
            update_table_entry(create_connection(), "build",
                               build_entry["build_id"], **build_entry)
        else:
            build_entry = build_schema
            build_entry["build_id"] = build_id if build_id is not None else str(uuid.uuid4())
            build_entry["container_name"] = container_name
            build_entry["definition_id"] = definition_id
            build_entry["container_type"] = to_format
            build_entry["container_owner"] = owner_id
            build_entry["build_status"] = "pending"
            create_table_entry(create_connection(), "build",
                               **build_entry)

        logging.info("Created build entry for {}".format(build_entry["build_id"]))

        if definition_entry["definition_type"] == "singularity" and to_format == "docker":
            update_table_entry(create_connection(), "build",
                               build_entry["build_id"], **{"build_status": "error"})
            raise ValueError("Can't build Docker container from Singularity file")

        update_table_entry(create_connection(), "build",
                           build_entry["build_id"], **{"build_status": "building"})
        if to_format == "docker":
            t0 = time.time()
            docker_image = build_to_docker(definition_entry, container_name)

            logging.info("Finished building {} in {} seconds".format(build_entry["build_id"],
                                                                     time.time() - t0))

            if docker_image:
                docker_client = docker.from_env()
                docker_image = docker_image[0]
                last_built = build_entry["build_time"] if build_entry["build_time"] else None
                build_time = datetime.datetime.now()
                for image in docker_client.df()["Images"]:
                    for repo_tag in image["RepoTags"]:
                        if container_name in repo_tag:
                            container_size = image["Size"]
                            break
                update_table_entry(create_connection(), "build",
                                   build_entry["build_id"], **{"build_status": "pushing",
                                                               "build_time": build_time,
                                                               "last_built": last_built,
                                                               "container_size": container_size})
                t0 = time.time()
                logging.info("Pushing {}".format(build_entry["build_id"]))
                response = push_to_ecr(docker_image, str(build_entry["build_id"]),
                                       container_name)
                logging.info("Finished pushing {} in {}".format(build_entry["build_id"],
                                                                time.time() - t0))
                print("!!!!!!!!!!!!!!!!!")
                print("PUSH RESPONSE: {}".format(response))
                print("!!!!!!!")
                if response is not None:
                    update_table_entry(create_connection(), "build",
                                       build_entry["build_id"], **{"build_status": "success"})
                    #TODO: When removing the image it leaves behind an image with no name.
                    docker_client.images.remove(response, force=True)
                    return build_entry["build_id"]
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
                singularity_image = build_to_singularity(definition_entry, container_name)
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
                return build_entry["build_id"]
            else:
                build_entry["build_status"] = "failed"
                update_table_entry(create_connection(), "build",
                                   build_entry["build_id"], **{"build_status": "failed"})
                raise ValueError("Failed to build singularity container")

    except Exception as e:
        print(e)
        logging.error("Exception", exc_info=True)
        return "Failed"


def pull_container(owner_id, build_id):
    """Pulls Docker containers from ECR and Singularity containers from S3.

    Parameters:
    owner_id (str): ID of definition file owner as returned by Globus Auth.
    token introspection.
    build_id (str): ID of container to pull.

    Return:
    (file obj.): File object of container.
    """
    try:
        build_entry = select_by_column(create_connection(), "build",
                                       build_id=build_id)
        if build_entry is not None and len(build_entry) == 1:
            build_entry = build_entry[0]

            if build_entry["container_owner"] != owner_id:
                return "You do not have access to this definition file"
        else:
            raise ValueError("Invalid build ID")
        if build_entry["container_type"] == "docker":
            registry = ecr_login()[8:] + "/" + build_id
            docker_client = docker.from_env()
            image = docker_client.images.pull(registry, tag=build_entry["container_name"])
            try:
                with open("temp.tar", "wb") as f:
                    for chunk in image.save():
                        f.write(chunk)

                return "temp.tar"
            except Exception as e:
                if os.path.exists("temp.tar"):
                    os.remove("temp.tar")
                print(e)
        else:
            try:
                s3 = boto3.client('s3')
                s3.download_file('xtract-container-service', os.path.join(build_entry["build_id"],
                                                                          build_entry["container_name"]),
                                 "temp.sif")
                return "temp.sif"
            except Exception as e:
                if os.path.exists("temp.sif"):
                    os.remove("temp.sif")
                print(e)
                return "Failed"


    except Exception as e:
        print("Exception: {}".format(e))
        logging.error("Exception", exc_info=True)



if __name__ == "__main__":
    logging.basicConfig(filename='app.log',
                        filemode='w',
                        level=logging.INFO, format='%(funcName)s - %(asctime)s - %(message)s')
    # build_container("d8959909-7efb-4319-813c-16403d602eed", "singularity", "lmao.sif")
