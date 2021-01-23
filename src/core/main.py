import argparse
import warnings

from utils import (
    init_aws_client,
    get_filenames,
    check_path_is_directory,
    upload_files,
    boilerplate_warning,
)


def main(
    root_path: str,
    bucket_name: str,
    aws_region_name: str = "us-east-2",
    recursive: bool = False,
    replace_if_exists: bool = False,
    aws_profile_name: str = None,
    key_prefix: str = None,
    extensions: tuple = None,
):
    """
    Main program entrypoint - runs the S3 video upload program.

    Parameters
    ----------
    root_path: str
        Root path where the files you want to upload to S3 are.

    bucket_name: str
        Name of the S3 bucket to upload to.

    aws_region_name: str (Optional, default is us-east-2 (Ohio))
        Name of the AWS region name to create the client object in.

    recursive: bool (Optional, default is True)
        Whether or not you want the program to recursively find files in subdirectories.

    replace_if_exists: bool (Optional, default is False)
        Whether to replace existing objects in S3 with a newer version.

    aws_profile_name: str (Optional)
        The name of the AWS profile to use - this will look in the ~/.aws/credentials file on your machine and
        use the credentials provided under the "aws_profile_name" entry.

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

    try:
        files = []

        boilerplate_warning()

        client = init_aws_client(
            service_name="s3", profile_name=aws_profile_name, region_name=aws_region_name
        )

        root_path_is_directory = check_path_is_directory(root_path=root_path)

        if root_path_is_directory:
            files = list(
                get_filenames(
                    root_path=root_path, recursive=recursive, extensions=extensions
                )
            )
        elif not root_path_is_directory:
            if recursive:
                warnings.warn(
                    message="Warning! Recursive flag does not change application state when uploading a single file!",
                    category=RuntimeWarning,
                )
            files = [root_path]

        print(files)

        upload_files(
            client=client,
            files=files,
            root_path=root_path,
            replace_if_exists=replace_if_exists,
            bucket_name=bucket_name,
            key_prefix=key_prefix,
            root_path_is_directory=root_path_is_directory,
        )

    except Exception as e:
        raise e


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parser for the S3 file upload program."
    )

    parser.add_argument(
        "root_path", help="Root path containing the files you wish to upload."
    )
    parser.add_argument(
        "bucket_name", help="The name of the S3 bucket you wish to upload files to."
    )
    parser.add_argument(
        "--aws-profile-name",
        required=False,
        help="The name of the AWS profile to use (optional).",
    )
    parser.add_argument(
        "--aws-region-name",
        required=False,
        default="us-east-2",
        help="The name of the AWS region to use (optional).",
    )
    parser.add_argument(
        "--key-prefix",
        required=False,
        help="The prefix to use for each file in the S3 bucket (optional).",
    )
    parser.add_argument(
        "-e",
        "--extensions",
        nargs="+",
        required=False,
        help="List of valid file extensions to upload (optional).",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        required=False,
        help="Whether to recursively search for files in subdirectories (optional).",
    )
    parser.add_argument(
        "--replace-if-exists",
        action="store_true",
        required=False,
        help="Whether the program will replace files that already exist in S3.",
    )

    args = parser.parse_args()

    main(
        root_path=args.root_path,
        bucket_name=args.bucket_name,
        replace_if_exists=args.replace_if_exists,
        recursive=args.recursive,
        aws_profile_name=args.aws_profile_name if args.aws_profile_name else None,
        aws_region_name=args.aws_region_name,
        key_prefix=args.key_prefix if args.key_prefix else None,
        extensions=tuple(args.extensions) if args.extensions else None,
    )
