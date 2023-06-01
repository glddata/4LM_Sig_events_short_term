from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, BlobClient


class AzureBlobStorage:
    """
    A class for connecting to and managing Azure Blob Storage.

    Args:
        connection_string (str): The connection string for the storage account.

    Raises:
        ValueError: If the connection string is invalid.
    """

    def __init__(self, connection_string: str) -> None:
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(
                connection_string
            )
        except Exception as e:
            raise ValueError(f"Failed to create BlobServiceClient: {e}")

    def list_containers(self) -> list[str]:
        """
        Lists all the containers in the storage account.

        Returns:
            list[str]: A list of container names.

        Raises:
            ValueError: If an error occurs while listing the containers.
        """
        try:
            containers = []
            for container in self.blob_service_client.list_containers():
                containers.append(container.name)
            return containers
        except Exception as e:
            raise ValueError(f"Failed to list containers: {e}")

    def list_blobs(self, container_name: str) -> list[str]:
        """
        Lists all the blobs in a container.

        Args:
            container_name (str): The name of the container.

        Returns:
            list[str]: A list of blob names.

        Raises:
            ValueError: If the container is not found, or if an error occurs while listing the blobs.
        """
        try:
            container_client = self.blob_service_client.get_container_client(
                container_name
            )
            blobs = []
            for blob in container_client.list_blobs():
                blobs.append(blob.name)
            return blobs
        except ResourceNotFoundError as e:
            raise ValueError(
                f"Failed to list blobs in container '{container_name}': Container not found: {e}"
            )
        except Exception as e:
            raise ValueError(
                f"Failed to list blobs in container '{container_name}': {e}"
            )

    def upload_blob(self, container_name: str, blob_name: str, data: bytes) -> None:
        """
        Uploads data to a blob in a container.

        Args:
            container_name (str): The name of the container.
            blob_name (str): The name of the blob.
            data (bytes): The data to upload.

        Raises:
            ValueError: If the container is not found, or if an error occurs while uploading the blob.
        """
        try:
            container_client = self.blob_service_client.get_container_client(
                container_name
            )
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.upload_blob(data)
        except ResourceNotFoundError as e:
            raise ValueError(
                f"Failed to upload blob '{blob_name}' to container '{container_name}': Container not found: {e}"
            )
        except Exception as e:
            raise ValueError(
                f"Failed to upload blob '{blob_name}' to container '{container_name}': {e}"
            )

    def download_blob(self, container_name: str, blob_name: str) -> bytes:
        """
        Downloads the contents of a blob as bytes.

        Args:
            container_name (str): The name of the container.
            blob_name (str): The name of the blob.

        Returns:
            bytes: The contents of the blob as bytes.

        Raises:
            ValueError: If the container or blob is not found, or if an error occurs while downloading the blob.
        """
        try:
            container_client = self.blob_service_client.get_container_client(
                container_name
            )
            blob_client = container_client.get_blob_client(blob_name)
            data = blob_client.download_blob().readall()
            return data
        except ResourceNotFoundError as e:
            raise ValueError(
                f"Failed to download blob '{blob_name}' from container '{container_name}': {e}"
            )
        except Exception as e:
            raise ValueError(
                f"Failed to download blob '{blob_name}' from container '{container_name}': {e}"
            )

        else:
            ...

    def delete_blob(self, container_name: str, blob_name: str) -> None:
        """
        Deletes a blob from a container.

        Args:
            container_name (str): The name of the container.
            blob_name (str): The name of the blob.

        Raises:
            ValueError: If the container or blob is not found, or if an error occurs while deleting the blob.
        """
        try:
            container_client = self.blob_service_client.get_container_client(
                container_name
            )
            blob_client = container_client.get_blob_client(blob_name)
            blob_client.delete_blob()
        except ResourceNotFoundError as e:
            raise ValueError(
                f"Failed to delete blob '{blob_name}' from container '{container_name}': Blob not found: {e}"
            )
        except Exception as e:
            raise ValueError(
                f"Failed to delete blob '{blob_name}' from container '{container_name}': {e}"
            )

    def create_container(self, container_name: str) -> None:
        """
        Creates a new container.

        Args:
            container_name (str): The name of the new container.

        Raises:
            ValueError: If a container with the same name already exists, or if an error occurs while creating the container.
        """
        try:
            self.blob_service_client.create_container(container_name)
        except Exception as e:
            if "ContainerAlreadyExists" in str(e):
                raise ValueError(f"Container '{container_name}' already exists") from e
            else:
                raise ValueError(f"Failed to create container '{container_name}': {e}")

    def delete_container(self, container_name: str) -> None:
        """
        Deletes a container.

        Args:
            container_name (str): The name of the container.

        Raises:
            ValueError: If the container is not found, or if an error occurs while deleting the container.
        """
        try:
            self.blob_service_client.delete_container(container_name)
        except ResourceNotFoundError as e:
            raise ValueError(
                f"Failed to delete container '{container_name}': Container not found: {e}"
            )
        except Exception as e:
            raise ValueError(f"Failed to delete container '{container_name}': {e}")
