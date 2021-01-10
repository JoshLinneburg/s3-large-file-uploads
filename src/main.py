import threading
import boto3
import os
import sys
from boto3.s3.transfer import TransferConfig

BUCKET_NAME = "movie-test-bucket"

session = boto3.Session(
    aws_access_key_id='YOUR_ACCESS_KEY_HERE',
    aws_secret_access_key='YOUR_SECRET_ACCESS_KEY_HERE'
    )

s3 = session.resource('s3')


def multi_part_upload_with_s3():
    # Multipart upload
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)

    file_path = 'D:/Media/Respawn/MP4s/50Jabba_the_Mom_Respawn_Inbox.mp4'

    key_path = 'D:/Media/Respawn/MP4s/50Jabba_the_Mom_Respawn_Inbox.mp4'

    s3.meta.client.upload_file(
        file_path,
        BUCKET_NAME,
        key_path,
        Config=config,
        Callback=ProgressPercentage(file_path)
        )


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


if __name__ == "__main__":
    multi_part_upload_with_s3()
