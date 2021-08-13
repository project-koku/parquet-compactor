import logging
import sys

import environ
from configurator import Configurator
from parquet_compactor import S3ParquetCompactor

root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
handler.setFormatter(formatter)
root.addHandler(handler)

LOG = logging.getLogger(__name__)


def main():
    """Run our compactor job."""
    environment = environ.Env()
    config_u_later = Configurator().configurator()
    s3_bucket_name = environment.get_value("REQUESTED_BUCKET")

    S3_BUCKET = config_u_later.get_object_store_bucket(s3_bucket_name)
    S3_ENDPOINT = config_u_later.get_object_store_endpoint()
    S3_DATA_PREFIX = config_u_later.get_data_prefix()
    AWS_ACCESS_KEY = config_u_later.get_object_store_access_key(s3_bucket_name)
    AWS_SECRET_ACCESS_KEY = config_u_later.get_object_store_secret_key(
        s3_bucket_name
    )

    compactor = S3ParquetCompactor(
        S3_BUCKET,
        S3_ENDPOINT,
        S3_DATA_PREFIX,
        aws_access_key=AWS_ACCESS_KEY,
        aws_secret_key=AWS_SECRET_ACCESS_KEY,
    )
    compactor.compact()


main()
