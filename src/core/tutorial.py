import boto3
import os
from boto3.s3.transfer import TransferConfig
from src.core import ProgressPercentage

BUCKET_NAME = "movie-test-bucket"
DIR_NAME = "D:/Media/Pictures/ST"
KEY_PREFIX = "Media/Pictures/ST"


session = boto3.Session(profile_name="personal")
s3_client = session.client("s3", region_name="us-east-2")


def multi_part_upload_with_s3():
    config = TransferConfig(
        multipart_threshold=1024 * 25,
        max_concurrency=10,
        multipart_chunksize=1024 * 25,
        use_threads=True,
    )

    for f in os.listdir(DIR_NAME):
        print(f)

        file_path = f"{DIR_NAME}/{f}"
        key_path = f"{KEY_PREFIX}/{f}"

        s3_client.upload_file(
            file_path,
            BUCKET_NAME,
            key_path,
            Config=config,
            Callback=ProgressPercentage(file_path),
        )


if __name__ == "__main__":
    multi_part_upload_with_s3()
