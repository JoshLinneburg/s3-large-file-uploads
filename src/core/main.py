import argparse

from src.core.utils import (
    init_aws_client,
    upload_file_to_s3,
    get_filenames,
)


def main(
    aws_profile_name,
    root_path,
    recursive,
    bucket_name,
    key_prefix=None,
    extensions=None,
):
    client = init_aws_client(
        service_name="s3", profile_name=aws_profile_name, region_name="us-east-1"
    )

    files = get_filenames(
        root_path=root_path, recursive=recursive, extensions=extensions
    )

    for file in files:
        print("\n")
        print(f"Uploading file {files.index(file) + 1} of {len(files)}")

        if key_prefix:
            key = file.split('/')[-1]
            key = f'{key_prefix}{key}'
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
