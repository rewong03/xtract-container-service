import os
import logging
import docker
import namegenerator
from spython.main import Client
from spython.main.parse.parsers import get_parser
from spython.main.parse.writers import get_writer
from db_handler import prep_database


#TODO: Singularity seems to kill the function if the container fails to build, might
# need some sort of extra step for logging to log errors
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

    logging.info("Successfully built %s Singularity container at %s",
                 os.path.basename(definition_file), container_location)


#TODO: Docker will require this script to be run with sudo privelages
# solution: https://askubuntu.com/questions/477551/how-can-i-use-docker-without-sudo
def build_to_docker(dockerfile, image_name):
    """Builds a Docker image from a Dockerfile.

    Parameters:
    dockerfile (str): Path to Dockerfile to build image from.
    image_name (str): Name to tag the final image with.
    """
    try:
        docker_client = docker.from_env()
        docker_client.images.build(path=os.path.dirname(dockerfile),
                                   tag=image_name)
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
    Client.load(os.path.dirname(dockerfile))
    Client.build(build_folder=os.path.dirname(container_location),
                 image=os.path.basename(container_location))

    logging.info("Successfully built %s Singularity container from %s at %s",
                 os.path.basename(container_location),
                 os.path.dirname(dockerfile),
                 container_location)


#TODO: Maybe find a better way to name converted Singularity definition files
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
    # prep_database("test.db")
    # db = "test.db"
    # build_to_singularity("test.def", "blah/my_test.sif")
    # build_to_docker("blah/Dockerfile", "tabular")
    # build_singularity_from_docker('./blah/Dockerfile', './tabby.sif')
    # convert_definition_file("blah/Dockerfile", "blah")

