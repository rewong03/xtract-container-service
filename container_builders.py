import datetime
import os
import logging
import shutil
import uuid
import boto3
import docker
import namegenerator
from io import BytesIO
from spython.main import Client
from spython.main.parse.parsers import get_parser
from spython.main.parse.writers import get_writer
from pg_utils import *



#TODO: Temporary fix for catching when singularity fails to build a definition file
# Usually singularity just prints the fail statement thorugh the singularity application
# instead of through python
def build_to_singularity(definition_file, container_location):
    """Builds a Singularity definition file at a location of the
    user's choice.

    Parameters:
    definition_file (str): Path to the file to be built.
    container_location (str): Path to location to build the container.
    """
    Client.load(definition_file)
    Client.build(build_folder=os.path.dirname(container_location),
                 image=os.path.basename(container_location))

    if os.path.exists(container_location):
        logging.info("Successfully built %s Singularity container at %s",
                     os.path.basename(definition_file), container_location)
    else:
        logging.error("Singularity failed to build %s", os.path.basename(definition_file))


#TODO: Docker will require this script to be run with sudo privelages
# Possible solution: https://askubuntu.com/questions/477551/how-can-i-use-docker-without-sudo
def build_to_docker(container_id, image_name):
    """Builds a Docker image from a Dockerfile.

    Parameters:
    container_id (str): id of entry in container table to build from.
    image_name (str): Name to tag the final image with.
    """
    try:
        bucket = boto3.resource('s3').Bucket("xtract-container-service")
        for object in bucket.objects.filter(Prefix=container_id):
            print(object)
            if not os.path.exists("./" + os.path.dirname(object.key)):
                os.makedirs("./" + os.path.dirname(object.key))
            bucket.download_file(object.key, "./" + object.key)

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
            build_entry["tag"] = image_name
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


def build_singularity_from_docker(dockerfile, container_location):
    """Builds a Singularity definition file at a location of the
        user's choice.

    Parameters:
    dockerfile (str): Path to Dockerfile to build image from.
    container_location (str): Path to location to build the container.
    """
    import boto3
    s3 = boto3.resource('s3')
    print(s3.Object('xtract-container-service', 'my_test/dockerfile').get()['Body'].read())
    # Client.load(dockerfile)
    # Client.build(build_folder=os.path.dirname(container_location),
    #              image=os.path.basename(container_location))
    #
    # if os.path.exists(container_location):
    #     logging.info("Successfully built %s Singularity container from %s at %s",
    #                  os.path.basename(container_location),
    #                  os.path.dirname(dockerfile),
    #                  container_location)
    # else:
    #     logging.error("Failed to build Singularity container from %s",
    #                   dockerfile)


#TODO: Find a better way to name converted Singularity definition files
def convert_definition_file(input_file, def_file_dir, singularity_def_name=None):
    """Converts a Dockerfile to a Singularity definition file or vice versa.

    Parameters:
    input_file (str): Path to definition file to convert.
    def_file_dir (str): Path to directory to place the converted definition file.
    singularity_def_name (str): Name to give to converted .def file if converting
    from Dockerfile to Singularity definition file.
    """
    if input_file.endswith(".def"):
        from_format = "Singularity"
        to_format = "docker"
    elif os.path.basename(input_file) == "Dockerfile":
        from_format = "docker"
        to_format = "Singularity"
    else:
        logging.error("Incorrect file types")
        return

    try:
        file_parser = get_parser(from_format)
        file_writer = get_writer(to_format)
        parser = file_parser(input_file)
        writer = file_writer(parser.recipe)
        result = writer.convert()

        if to_format == "Singularity":
            if singularity_def_name is None:
                singularity_def_name = namegenerator.gen() + ".sif"

            file_path = os.path.join(def_file_dir, singularity_def_name)
        else:
            file_path = os.path.join(def_file_dir, "Dockerfile")

        with open(file_path, 'w') as f:
            f.write(result)

        logging.info("Successfully converted %s %s definition file to %s %s definition file",
                     os.path.basename(input_file),
                     from_format,
                     os.path.basename(file_path),
                     to_format)

    except Exception as e:
        logging.error("Exception", exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(filename='app.log', filemode='w',
                        level=logging.INFO, format='%(funcName)s - %(asctime)s - %(message)s')
    # build_to_singularity("blah.def", "./blah.sif")
    # prep_database("test.db")
    # db = "test.db"
    # build_to_singularity("test.def", "blah/my_test.sif")
    # build_to_docker("d782ffbf-84de-447f-91ce-b29c6142ba76", "bad-image")
    # build_singularity_from_docker('/Users/ryan/Documents/CS/CDAC/singularity-vm/xtract-container-service/Dockerfile', './tabby.sif')
    # convert_definition_file("blah/Dockerfile", "blah")

