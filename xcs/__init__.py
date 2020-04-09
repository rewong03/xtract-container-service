import json
import requests


class XtractConnection:
    """Class to interact with XCS.

    Parameters:
    funcx_token (str): Token for FuncX retrieved through Globus Auth.
    base_url (str): URL of
    """
    def __init__(self, funcx_token, base_url="http://ec2-54-213-90-206.us-west-2.compute.amazonaws.com"):
        self.headers = {'Authorization': f"Bearer {funcx_token}"}
        self.base_url = base_url

    def register_container(self, file_name, file_path):
        """Registers and stores a Docker or Singularity definition file.

        Parameters:
        file_name (str): Name of definition file.
        file_path (str): Path of file to send.

        Returns:
        definition_id (str): ID of the uploaded definition file or an error message.
        """
        url = "http://127.0.0.1:5000/upload_def_file"
        payload = {"file": (file_name, open(file_path, "rb"))}
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
        url = "http://127.0.0.1:5000/build"
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
        url = "http://127.0.0.1:5000/build"
        payload = {"build_id": build_id}
        response = requests.get(url, json=payload, headers=self.headers)

        try:
            status = json.loads(response.text)
        except:
            status = response.text

        return status
