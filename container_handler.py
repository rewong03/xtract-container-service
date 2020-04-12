import datetime
import logging
import os
import shutil
import subprocess
import tarfile
import time
import tempfile
import uuid
import zipfile
import boto3
import docker
import namegenerator
from spython.main import Client
from spython.main.parse.parsers import get_parser
from spython.main.parse.writers import get_writer
from pg_utils import definition_schema, build_schema, create_table_entry, update_table_entry, select_by_column
from sqs_queue_utils import put_message


def pull_s3_dir(definition_id):
    """Pulls a directory of files from a definition_id folder in our
    S3 bucket.

    Parameters:
    definition_id (str): Name of id to pull files from.
    """
    bucket = boto3.resource('s3').Bucket("xtract-container-service")
    for object in bucket.objects.filter(Prefix=definition_id):
        if not os.path.exists("./" + os.path.dirname(object.key)):
            os.makedirs("./" + os.path.dirname(object.key))
        bucket.download_file(object.key, "./" + object.key)


def ecr_login():
    """Logs Docker into ECR registry.

    Returns:
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

    Returns:
    (str): ID of pushed Docker image or None if the push fails.
    """
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
        raise ValueError("Failed to push")


def build_to_singularity(definition_entry, container_location):
    """Builds a Singularity container from a Dockerfile or Singularity file
    within the definition db.

    Parameters:
    definition_entry (str): Entry of definition db entry to build singularity container from.
    container_location (str): Path to location to build the container.

    Returns:
    container_location: Returns the location of the Singularity container or None if it
    fails to save.
    """
    definition_id = definition_entry["definition_id"]
    pull_s3_dir(definition_id)
    Client.load("./" + definition_id)
    Client.build(image=os.path.join("./", container_location))
    shutil.rmtree("./" + definition_id)
    #TODO Find a better way to error check
    if os.path.exists(container_location):
        logging.info("Successfully built {}".format(container_location))
        return container_location
    else:
        return None


def build_to_docker(definition_entry, image_name):
    """Builds a Docker image from a definition db entry.

    Parameters:
    definition_entry (str): Entry of definition db entry to build docker container from.
    image_name (str): Name to tag the final image with.

    Returns:
    image (Image obj.): Docker image object or None if the container fails to build.
    """
    definition_id = definition_entry["definition_id"]
    pull_s3_dir(definition_id)

    try:
        docker_client = docker.from_env()
        image = docker_client.images.build(path="./{}".format(definition_id),
                                           tag=image_name, rm=True, forcerm=True)
        return image
    except:
        return None
    finally:
        if os.path.exists(definition_id):
            shutil.rmtree("./" + definition_id)


#TODO: Find a better way to name converted Singularity definition files
def convert_definition_file(definition_id, singularity_def_name=None):
    """Converts a Dockerfile0 to a Singularity definition file or vice versa.

    Parameters:
    definition_id (str): ID of definitio db entry to convert.
    singularity_def_name (str): Name to give to converted .def file if converting
    from Dockerfile0 to Singularity definition file.
    """
    try:
        definition_entry = select_by_column("definition", definition_id=definition_id)

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
        db_entry["location"] = "s3"
        create_table_entry("container", **db_entry)

        logging.info("Successfully converted %s %s definition file to %s %s definition file",
                     os.path.basename(input_file),
                     from_format,
                     os.path.basename(file_path),
                     to_format)

    except Exception as e:
        logging.error("Exception", exc_info=True)
    finally:
        shutil.rmtree(new_path)


def build_container(build_entry, to_format, container_name):
    """Automated pipeline for building a recipe file from the
    definition db to a container.

    Parameters:
    build_entry (dict): Build entry from PostgreSQl of container to build.
    to_format (str): Format of container to build. Either "singularity"
    or "docker". If "docker", the recipe type must be a Dockerfile0.
    container_name (str): Name to give the container or path for path for
    Singularity container.
    build_id (str): UUID to give to a new build entry. build_id should be None if
    a build entry exists for definition_id.

    Returns:
    build_id (str): Build id of the built container or failed if the container
    failed to build.
    """
    try:
        definition_id = build_entry["definition_id"]
        build_id = build_entry["build_id"]
        definition_entry = select_by_column("definition", definition_id=definition_id)[0]

        logging.info("Created build entry for {}".format(build_id))

        if definition_entry["definition_type"] == "singularity" and to_format == "docker":
            update_table_entry("build", build_id, **{"build_status": "error"})
            raise ValueError("Can't build Docker container from Singularity file")

        update_table_entry("build", build_id, **{"build_status": "building"})
        if to_format == "docker":
            t0 = time.time()
            docker_image = build_to_docker(definition_entry, container_name)
            if docker_image:
                docker_client = docker.from_env()
                docker_image = docker_image[0]
                last_built = build_entry["build_time"] if build_entry["build_time"] else None
                build_time = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                for image in docker_client.df()["Images"]:
                    if any(list(map(lambda x: container_name in x, image["RepoTags"]))):
                        container_size = image["Size"]
                        break
                    else:
                        container_size = None
                update_table_entry("build", build_id, **{"build_status": "pushing",
                                                         "build_time": build_time,
                                                         "last_built": last_built,
                                                         "container_size": container_size})
                logging.info("Built {} in {} seconds".format(build_id, time.time() - t0))
                t0 = time.time()
                logging.info("Pushing {}".format(build_id))
                response = push_to_ecr(docker_image, build_id,
                                       container_name)
                logging.info("Finished pushing {} in {}".format(build_id,
                                                                time.time() - t0))
                if response is not None:
                    update_table_entry("build", build_id, **{"build_status": "success"})
                    docker_client.images.remove(response, force=True)
                    return build_id
                else:
                    docker_client.images.remove(container_name, force=True)
                    raise ValueError("Failed to push")
            else:
                raise ValueError("Failed to build docker container")

        elif to_format == "singularity":
            if container_name.endswith(".sif"):
                singularity_image = build_to_singularity(definition_entry, container_name)
            else:
                raise ValueError("Invalid Singularity container name")
            if singularity_image:
                build_time = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
                last_built = build_entry["build_time"] if build_entry["build_time"] else None
                image_size = os.path.getsize(container_name)

                build_entry["build_status"] = "pushing"
                update_table_entry("build", build_id, **{"build_status": "pushing",
                                                         "build_time": build_time,
                                                         "last_built": last_built,
                                                         "container_size": image_size})
                s3 = boto3.client("s3")
                s3.upload_fileobj(open(singularity_image, 'rb'),
                                  "xtract-container-service",
                                  "{}/{}".format(build_id,
                                                 os.path.basename(container_name)))
                update_table_entry("build", build_id, **{"build_status": "success"})
                os.remove(container_name)
                return build_id
            else:
                raise ValueError("Failed to build singularity container")

    except Exception as e:
        print("{}: {}".format(build_entry["build_id"], e))
        logging.error("Exception", exc_info=True)

        if build_entry is not None and len(build_entry) == 1:
            build_entry["build_status"] = "failed"
            update_table_entry("build", build_entry["build_id"], **{"build_status": "failed"})

        raise e


def pull_container(build_entry):
    """Pulls Docker containers from ECR and Singularity containers from S3.

    Parameters:
    owner_id (str): ID of definition file owner as returned by Globus Auth.
    token introspection.
    build_id (str): ID of container to pull.

    Returns:
    (file obj.): File object of container.
    """
    build_id = build_entry["build_id"]
    file_name = build_id + (".tar" if build_entry["container_type"] == "docker" else ".sif")

    try:
        if build_entry["container_type"] == "docker":
            registry = ecr_login()[8:] + "/" + build_id
            docker_client = docker.from_env()
            image = docker_client.images.pull(registry, tag=build_entry["container_name"])

            with open(file_name, "wb") as f:
                for chunk in image.save():
                    f.write(chunk)

            return file_name
        elif build_entry["container_type"] == "singularity":
            s3 = boto3.client('s3')
            s3.download_file('xtract-container-service', os.path.join(build_entry["build_id"],
                                                                      build_entry["container_name"]),
                             file_name)
            return file_name
    except Exception as e:
        print("{}: {}".format(build_id, e))
        if os.path.exists(file_name):
            os.remove(file_name)
        return None


def repo2docker_container(client_id, build_id, target, container_name):
    """Takes a .zip or .tar file object or git repo link and attempts to run repo2docker on it.

    Note:
    The definition information for a container will only be stored if the container successfully
    builds.

    Parameters:
    target (file obj. or str.): A link to a github repository or a file object.
    container_name (str): Name to give to container.
    """
    if isinstance(target, str) and target.startswith("https://github.com"):
        target_type = "git"
        temp_dir = ""
        cmd = "jupyter-repo2docker --no-run --image-name {} {}".format(container_name, target)
    else:
        file_obj = open(target, "rb")
        if zipfile.is_zipfile(file_obj):
            target_type = ".zip"
            with zipfile.ZipFile(file_obj) as zip_obj:
                temp_dir = tempfile.mkdtemp()
                zip_obj.extractall(path=temp_dir)
        else:
            try:
                # For some reason literally any file will pass through this tarfile check
                with tarfile.TarFile(fileobj=file_obj) as tar_obj:
                    temp_dir = tempfile.mkdtemp()
                    tar_obj.extractall(path=temp_dir)

                if len(os.listdir(temp_dir)) == 0:
                    os.removedirs(temp_dir)
                    return "Failed"
                target_type = ".tar"
            except tarfile.TarError:
                os.remove(target)
                return "Failed"

        cmd = "jupyter-repo2docker --no-run --image-name {} {}".format(container_name, temp_dir)
    build_entry = build_schema
    build_entry["build_id"] = build_id
    build_entry["container_name"] = container_name
    build_entry["container_type"] = "docker"
    build_entry["container_owner"] = client_id
    build_entry["build_status"] = "building"
    build_entry["build_time"] = datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    create_table_entry("build", **build_entry)

    subprocess.call(cmd, shell=True)
    client = docker.from_env()
    try:
        docker_image = client.images.get(container_name)
        update_table_entry("build", build_id, build_status="pushing")
    except:
        update_table_entry("build", build_id, build_status="failed")
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        if os.path.exists(target):
            os.remove(target)
        return "Failed"

    definition_id = str(uuid.uuid4())
    create_table_entry("definition",
                       definition_id=definition_id,
                       definition_type="docker",
                       definition_name=container_name,
                       location=target if target_type == "git" else "s3",
                       definition_owner=client_id)

    if target_type == ".zip" or target_type == ".tar":
        s3 = boto3.client('s3')

        s3.upload_fileobj(file_obj, "xtract-container-service",
                          '{}/{}'.format(definition_id, container_name + target_type))

    for image in client.df()["Images"]:
        if any(list(map(lambda x: container_name in x, image["RepoTags"]))):
            container_size = image["Size"]
            break
        else:
            container_size = None

    update_table_entry("build", build_id, definition_id=definition_id, container_size=container_size)

    response = push_to_ecr(docker_image, build_id, container_name)

    if response is not None:
        update_table_entry("build", build_id, **{"build_status": "success"})
        client.images.remove(response, force=True)
    else:
        client.images.remove(container_name, force=True)
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        if os.path.exists(target):
            os.remove(target)
        return "Failed"

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    if os.path.exists(target):
        os.remove(target)

    return build_id


if __name__ == "__main__":
    from task_manager import TaskManager

    manager = TaskManager(max_threads=2, idle_time=10, kill_time=20)
    function_name = "build_container"
    build_entry = select_by_column("build", build_id="48233d24-de38-45d3-988c-25285bc2a7ad")[0]
    print(put_message({"function_name": function_name,
                       "build_entry": build_entry,
                       "to_format": "docker",
                       "container_name": "my_thread_test"}))
    print("Continuing")
    function_name = "build_container"
    build_entry = select_by_column("build", build_id="aab75744-597e-4bdb-93ad-6e06b139bafd")[0]
    print(put_message({"function_name": function_name,
                       "build_entry": build_entry,
                       "to_format": "docker",
                       "container_name": "my_thread_test"}))
    manager.start_thread()
    manager.start_thread()
    manager.start_thread()

    for i in range(1000):
        print(manager.thread_status)
        time.sleep(2)

    # for i in range(10):
    #     x = threading.Thread(target=test, args=(i,))
    #     x.start()
    # print('cool')
    pass