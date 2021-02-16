import boto3
import os
import sys
import logging
import threading
import time
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

CONFIG = TransferConfig(
    multipart_threshold=os.environ.get("multipart_threshold") or 1024 * 25,
    max_concurrency=os.environ.get("max_concurrency") or 10,
    multipart_chunksize=os.environ.get("multipart_chunksize") or 1024 * 25,
    use_threads=os.environ.get("use_threads") or True,
)

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
logger = logging.getLogger()


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


def check_object_exists(
    s3_client: boto3.client, bucket_name: str, object_key: str
) -> bool:
    """
    Checks whether an object already exists in AWS S3.

    Parameters
    ----------
    s3_client: boto3.client
        Boto3 S3 client.

    bucket_name: str
        Name of the S3 bucket to check for the object.

    object_key: str
        Name of the object in the bucket_name S3 Bucket.

    Returns
    -------
    bool
        Whether or not the object exists in the S3 location specified.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        return True
    except s3_client.exceptions.NoSuchKey:
        return False
    except ClientError as e:
        if e.response["Error"]["Code"] == "InvalidObjectState":
            logger.warning("Object exists but is in invalid state")
            return True
        else:
            return False


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
    try:

        with open(path_to_file, "rb") as f:
            s3_client.upload_fileobj(
                Fileobj=f,
                Bucket=bucket_name,
                Key=object_key,
                Callback=ProgressPercentage(path_to_file),
                Config=CONFIG,
            )

    except Exception as e:
        raise e


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
    try:
        files_to_return = []

        if extensions:
            files = [file for file in files if file.endswith(extensions)]
        for file in files:
            files_to_return.append(os.path.join(root_path, file).replace("\\", "/"))

        return files_to_return

    except Exception as e:
        raise e


def get_filenames_flat(root_path: str, extensions: tuple = None) -> list:
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
    try:
        files = [
            name
            for name in os.listdir(root_path)
            if not os.path.isdir(os.path.join(root_path, name))
        ]

        return list(
            clean_file_list(root_path=root_path, files=files, extensions=extensions)
        )

    except Exception as e:
        raise e


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
    try:
        files_to_return = []

        for root, dirs, files in os.walk(top=root_path):
            files_to_return += clean_file_list(
                root_path=root, files=files, extensions=extensions
            )

        return files_to_return

    except Exception as e:
        raise e


def get_filenames(
    root_path: str, recursive: bool = False, extensions: tuple = None
) -> list:
    """
    Gets the filenames and absolute paths for those files from
    the root_path specified.

    Parameters
    ----------
    root_path: str
        Absolute root file path that contains files you wish to retrieve.

    recursive: bool (Optional, default = False)
        Whether you want to recursively retrieve files in subdirectories or not.

    extensions: tuple (Optional)
        Valid file extensions to be returned.

    Returns
    -------
    files: list
        Files from the root_path with the desired extensions.
    """

    try:

        if recursive:
            files = get_filenames_recursive(root_path=root_path, extensions=extensions)
        else:
            files = get_filenames_flat(root_path=root_path, extensions=extensions)

        return list(files)

    except Exception as e:
        raise e


def check_path_is_directory(root_path):
    """
    Checks whether the root_path is a directory, a file, or does not exist.

    Parameters
    ----------
    root_path: str
        Path on the filesystem.

    Returns
    -------
    bool
        Whether the path is a directory (True), a file (False).
        If the path does not exist, the program raises a FileNotFoundError.
    """
    if os.path.isdir(root_path):
        return True
    elif os.path.exists(root_path):
        return False
    else:
        raise FileNotFoundError(f"{root_path} does not exist as a file or directory!")


def upload_files(
    client: boto3.client,
    files: list,
    root_path: str,
    replace_if_exists: bool,
    root_path_is_directory: bool,
    bucket_name: str,
    key_prefix: str = None,
):
    """
    Uploads a list of files to S3.

    Parameters
    ----------
    client: boto3.client
        Boto3 S3 client.

    files: list
        List of filenames to upload

    root_path: str
        The root path where the files are located. In the case of single-file uploads,
        this is just the location of the file.

    replace_if_exists: bool
        Whether to replace files that already exist in S3.

    root_path_is_directory: bool
        Whether the root_path passed in is a directory or a single file.

    bucket_name: str
        The name of the destination S3 Bucket.

    key_prefix: str (Optional)
        The key prefix of the files you wish to upload. If you do not specify this,
        the files will be uploaded using the absolute path from your computer.

        E.g., if this is not passed in, your files will be located as C:/Users/path/to/files/
        in the S3 bucket.

    Returns
    -------
    None
    """
    try:
        for file in files:
            print(f"Uploading file {files.index(file) + 1} of {len(files)}")

            if key_prefix and root_path_is_directory:
                key = file.replace(root_path, key_prefix)
            elif key_prefix and not root_path_is_directory:
                key = f"{key_prefix}{file.split('/')[-1]}"
            else:
                key = file

            file_exists = check_object_exists(
                s3_client=client, bucket_name=bucket_name, object_key=key
            )

            if (not file_exists) or (file_exists and replace_if_exists):
                print(f"Destination: s3://{bucket_name}/{key}")
                upload_file_to_s3(
                    s3_client=client,
                    path_to_file=file,
                    bucket_name=bucket_name,
                    object_key=key,
                )
            else:
                print("File already exists in S3 and will not be replaced.")

            print('\n')

    except Exception as e:
        raise e


def boilerplate_warning():
    """
    Warns the user about S3 data usage - prompts them for a "Y" or "N" response
    before allowing the program to continue.

    If the user elects to not continue with the upload, responding with an "N",
    the program exits.

    Returns
    -------
    None
    """
    warning_string = (
        "\nWarning! I am not responsible for any costs incurred on your personal AWS account for data storage.\n"
        "You are responsible for managing your own S3 files once they are uploaded."
    )

    print(warning_string)

    while True:
        response = input("Do you want to continue? Enter 'y' or 'n'.")

        if response.lower().strip() not in ("y", "n", "yes", "no"):
            print("Invalid response.")
            continue
        elif response.lower().strip() in ("y", "yes"):
            print("\n")
            break
        elif response.lower().strip() in ("n", "no"):
            exit()
