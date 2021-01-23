import argparse

from src.core.utils import (
    init_aws_client,
    upload_file_to_s3,
    get_filenames,
)


def main(
    root_path,
    recursive,
    bucket_name,
    aws_profile_name=None,
    key_prefix=None,
    extensions=None,
):
    """
    Main program entrypoint - runs the S3 video upload program.

    Parameters
    ----------
    root_path: str
        Root path where the files you want to upload to S3 are.

    recursive: bool
        Whether or not you want the program to recursively find files in subdirectories.

    bucket_name: str
        Name of the S3 bucket to upload to.

    aws_profile_name: str (Optional)
        The name of the AWS profile to use - this will look in the ~/.aws/credentials
        file on your machine and use the credentials provided under the "aws_profile_name"
        entry.

        If running on your personal machine, you must specify this parameter.

    key_prefix: str (Optional)
        The key prefix of the files you wish to upload. If you do not specify this,
        the files will be uploaded using the absolute path from your computer.

        E.g., if this is not passed in, your files will be located as C:/Users/path/to/files/
        in the S3 bucket.

    extensions: tuple (Optional)
        Valid file extensions to be uploaded.

    Returns
    -------
    None
    """
    client = init_aws_client(
        service_name="s3", profile_name=aws_profile_name, region_name="us-east-1"
    )

    files = get_filenames(
        root_path=root_path, recursive=recursive, extensions=extensions
    )

    for file in files:
        print(f"Uploading file {files.index(file) + 1} of {len(files)}")

        if key_prefix:
            key = file.split("/")[-1]
            key = f"{key_prefix}{key}"
        else:
            key = file

        upload_file_to_s3(
            s3_client=client,
            path_to_file=file,
            bucket_name=bucket_name,
            object_key=key,
        )


if __name__ == "__main__":
    main(
        aws_profile_name="personal",
        root_path="E:/Videos/Movies",
        recursive=False,
        bucket_name="movie-test-bucket",
        key_prefix="test-movies/",
    )
