import argparse

from utils import (
    init_aws_client,
    upload_file_to_s3,
    get_filenames,
)


def main(
    root_path,
    bucket_name,
    recursive=False,
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

    bucket_name: str
        Name of the S3 bucket to upload to.

    recursive: bool (Optional, default is True)
        Whether or not you want the program to recursively find files in subdirectories.

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
            break
        elif response.lower().strip() in ("n", "no"):
            exit()

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
        "-r",
        "--recursive",
        action="store_true",
        required=False,
        help="Whether to recursively search for files in subdirectories (optional).",
    )

    args = parser.parse_args()

    main(
        root_path=args.root_path,
        bucket_name=args.bucket_name,
        recursive=args.recursive,
        aws_profile_name=args.aws_profile_name if args.aws_profile_name else None,
        key_prefix=args.key_prefix if args.key_prefix else None,
        extensions=tuple(args.extensions) if args.extensions else None,
    )
