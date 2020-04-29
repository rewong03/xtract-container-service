import json
import requests


class XtractConnection:
    """Class to interact with XCS.

    Parameters:
    funcx_token (str): Token for FuncX retrieved through Globus Auth.
    base_url (str): URL of server running XCS.
    """
    def __init__(self, funcx_token, base_url="http://149.165.168.132"):
        self.headers = {'Authorization': f"Bearer {funcx_token}"}
        self.base_url = base_url

    def register_container(self, file_name, file_obj):
        """Registers and stores a Docker or Singularity definition file.

        Parameters:
        file_name (str): Name of definition file.
        file_obj: File object of definition file.

        Returns:
        definition_id (str): ID of the uploaded definition file or an error message.
        """
        url = f"{self.base_url}/upload_def_file"
        payload = {"file": (file_name, file_obj)}
        response = requests.post(url, files=payload, headers=self.headers)
        definition_id = response.text

        return definition_id

    def build(self, definition_id, to_format, container_name):
        """Builds a Docker or Singularity container from an uploaded definition file.

        Note:
        A Docker definition file can be built into a Docker and Singularity container but a
        Singularity definition file can only be built into a Singularity container.

        Parameters:
        definition_id (str): ID of definition file to build from.
        to_format (str): "singularity" or "docker".
        container_name (str): Name to give the built container.

        Returns:
        build_id (str): ID of the container being built or an error message.
        """
        url = f"{self.base_url}/build"
        payload = {"definition_id": definition_id, "to_format": to_format, "container_name": container_name}
        response = requests.post(url, json=payload, headers=self.headers)
        build_id = response.text

        return build_id

    def get_status(self, build_id):
        """Retrieves the build entry of a build_id

        Parameters:
        build_id (str): ID of build entry to get.

        Returns:
        status (json or str.): Json of build entry or an error message
        """
        url = f"{self.base_url}/build"
        payload = {"build_id": build_id}
        response = requests.get(url, json=payload, headers=self.headers)

        try:
            status = json.loads(response.text)
        except:
            status = response.text

        return status

    def pull(self, build_id, file_path):
        """Pulls a container down and writes it to a file.

        Note:
        Docker containers are pulled as .tar files and Singularity containers
        are pulled as .sif files.

        Parameters:
        build_id (str): ID of build to pull down.
        file_path (str): Full path of file to write to.

        Returns:
        (str): A success or error message.
        """
        url = f"{self.base_url}/pull"
        payload = {"build_id": build_id}
        response = requests.get(url, json=payload, headers=self.headers)
        if response.headers["Content-Type"] == "text/html":
            return response.text
        else:
            with open(file_path, 'wb') as f:
                f.write(response.content)

            return "Success"

    def repo2docker(self, container_name, git_repo=None, file_obj=None):
        """Builds a Docker container from a git repository or .tar or .zip file.

        Parameters:
        container_name (str): Name of container to build.
        git_repo (str): URL to base git repository to build.
        file_obj: Binary file object of .zip or .tar file to build.

        Return:
        (str): build_id of container or an error message.
        """
        url = f"{self.base_url}/repo2docker"

        if git_repo and file_obj:
            return "Can only upload a git repository OR a file"
        elif git_repo:
            payload = {"container_name": container_name, "git_repo": git_repo}
            response = requests.post(url, json=payload, headers=self.headers)
            build_id = response.text

            return build_id
        elif file_obj:
            payload = {"file": (container_name, file_obj)}
            response = requests.post(url, files=payload, headers=self.headers)
            build_id = response.text

            return build_id
        else:
            return "No git repository or file path"

    def convert(self, definition_id, singularity_def_name=None):
        """Converts a Dockerfile to a Singularity recipe or vice versa.

        Parameters:
        definition_id (str): Definition entry ID of file to convert.
        singularity_def_name (str): Name to give to Singularity recipe files
        converted from Dockerfiles. A random name is chosen if no name is supplied.

        Returns:
        (str): The ID of the new definition entry or an error message.
        """
        url = f"{self.base_url}/convert"

        payload = {"definition_id": definition_id, "singularity_def_name": singularity_def_name}
        response = requests.post(url, json=payload, headers=self.headers)
        new_definition_id = response.text

        return new_definition_id
