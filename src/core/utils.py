import boto3
import os
import sys
import threading
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from typing import Iterable

CONFIG = TransferConfig(
    multipart_threshold=os.environ.get("multipart_threshold") or 1024 * 25,
    max_concurrency=os.environ.get("max_concurrency") or 10,
    multipart_chunksize=os.environ.get("multipart_chunksize") or 1024 * 25,
    use_threads=os.environ.get("use_threads") or True,
)


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)"
                % (self._filename, self._seen_so_far, self._size, percentage)
            )
            sys.stdout.flush()


def init_aws_client(
    service_name: str, profile_name: str = None, region_name: str = "us-east-1"
) -> boto3.client:
    """
    Creates a client for the AWS service in the region specified.

    Parameters
    ----------
    service_name: str
        Name of the AWS service you would like to access.

    profile_name: str (Optional)
        Name of the AWS profile you wish to use.

    region_name: str (Optional)
        Region to access the service in. Defaults to 'us-east-1'.

    Raises
    -------
    e: boto3.ClientError

    Returns
    -------
    boto3.client: Client of the resource you would like to access.
    """
    try:

        if profile_name:
            session = boto3.Session(profile_name=profile_name)
            client = session.client(service_name=service_name, region_name=region_name)
        else:
            client = boto3.client(service_name, region_name=region_name)

        if client:
            return client
        else:
            raise ClientError

    except ClientError as e:
        raise e


def upload_file_to_s3(
    s3_client: boto3.client, path_to_file: str, bucket_name: str, object_key: str
) -> None:
    """
    Uploads a local file to AWS S3.

    Parameters
    ----------
    s3_client: boto3 Client
        AWS low-level Client to be used to transfer data to S3.

    path_to_file: str
        Local file to be uploaded to S3 - includes the path to the file and extension.

    bucket_name: str
        Name of the S3 bucket where the file should be moved to.

    object_key: str
        Name of the file once it lands in the S3 bucket.

    Returns
    -------
    None
    """
    with open(path_to_file, "rb") as f:
        s3_client.upload_fileobj(
            Fileobj=f,
            Bucket=bucket_name,
            Key=object_key,
            Callback=ProgressPercentage(path_to_file),
            Config=CONFIG,
        )


def clean_file_list(root_path: str, files: list, extensions: tuple = None) -> list:
    """
    Removes files from a file list if they don't end with the given extensions.
    Any files that remain are joined to the root_path to provide a complete path
    to the file.

    Parameters
    ----------
    root_path: str
        Absolute root file path that contains files.

    files: list
        List of file names.

    extensions: tuple (Optional)
        Valid file extensions to be returned.

    Returns
    -------
    files_to_return: list
        List of absolute path file names of the desired extension.
    """
    files_to_return = []

    if extensions:
        files = [file for file in files if file.endswith(extensions)]
    for file in files:
        files_to_return.append(os.path.join(root_path, file).replace("\\", "/"))

    return files_to_return


def get_filenames_flat(root_path: str, extensions: tuple = None) -> Iterable:
    """
    Gathers up all filenames in a directory - does not return filenames
    contained in subdirectories.

    Parameters
    ----------
    root_path: str
        Absolute root file path that contains files you wish to retrieve.

    extensions: tuple (Optional)
        Valid file extensions to be returned.

    Returns
    -------
    files_to_return: list
        List of absolute file paths for all files in all directories under root_path.
    """
    files = [
        name
        for name in os.listdir(root_path)
        if not os.path.isdir(os.path.join(root_path, name))
    ]

    return clean_file_list(root_path=root_path, files=files, extensions=extensions)


def get_filenames_recursive(root_path: str, extensions: tuple = None) -> list:
    """
    Gathers up all filenames under a root directory recursively.

    Parameters
    ----------
    root_path: str
        Absolute root file path that contains files you wish to retrieve.

    extensions: tuple (Optional)
        Valid file extensions to be returned.

    Returns
    -------
    files_to_return: list
        List of absolute file paths for all files in all directories under root_path.
    """
    files_to_return = []

    for root, dirs, files in os.walk(top=root_path):
        files_to_return += clean_file_list(
            root_path=root, files=files, extensions=extensions
        )

    return files_to_return


def get_filenames(
    root_path: str, recursive: bool = True, extensions: tuple = None
) -> Iterable:
    """
    Gets the filenames and absolute paths for those files from
    the root_path specified.

    Parameters
    ----------
    root_path: str
        Absolute root file path that contains files you wish to retrieve.

    recursive: bool (Optional, default = True)
        Whether you want to recursively retrieve files in subdirectories or not.

    extensions: tuple (Optional)
        Valid file extensions to be returned.

    Returns
    -------
    files: list
        Files from the root_path with the desired extensions.
    """
    if recursive:
        files = get_filenames_recursive(root_path=root_path, extensions=extensions)
    else:
        files = get_filenames_flat(root_path=root_path, extensions=extensions)

    return files
