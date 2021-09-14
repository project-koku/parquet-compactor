import logging
import math

import awswrangler as wr
import boto3
import environ
from awswrangler.exceptions import EmptyDataFrame
from pyarrow import ArrowException


LOG = logging.getLogger(__name__)

TARGET_FILE_SIZE_GB = environ.Env().get_value(
    "TARGET_FILE_SIZE_GB", cast=float, default=0.5
)
FILE_SIZE_BYTES = TARGET_FILE_SIZE_GB * pow(2, 30)


class S3ParquetCompactor:
    """Compact Parquet files in S3 based on their path."""

    def __init__(
        self,
        bucket,
        endpoint,
        data_prefix,
        aws_access_key=None,
        aws_secret_key=None,
    ) -> None:
        if aws_access_key and aws_secret_key:
            self.session = boto3.Session(
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
            )
            self.client = self.session.client("s3", endpoint_url=endpoint)
            self.s3 = self.session.resource("s3", endpoint_url=endpoint)
        else:
            self.session = None
            self.client = boto3.client("s3", endpoint_url=endpoint)
            self.s3 = boto3.resource("s3", endpoint_url=endpoint)
        self.bucket = bucket
        self.path_prefix = f"s3://{self.bucket}/"
        self.data_prefix = data_prefix
        wr.config.s3_endpoint_url = endpoint
        msg = f"Initialzed S3ParquetCompactor for {self.path_prefix}"
        LOG.info(msg)

    def get_common_prefixes(self, prefix) -> list:
        """Return common prefixes in the bucket"""
        prefixes = []
        paginator = self.client.get_paginator("list_objects")
        result = paginator.paginate(
            Bucket=self.bucket, Prefix=prefix, Delimiter="/"
        )
        for prefix in result.search("CommonPrefixes"):
            if prefix:
                prefixes.append(prefix.get("Prefix"))
        return prefixes

    def get_common_prefixes_recursive(self, prefix) -> list:
        """Return file lists at the leaf nodes in the bucket"""
        results = []
        paginator = self.client.get_paginator("list_objects_v2")
        result = paginator.paginate(
            Bucket=self.bucket, Prefix=prefix, Delimiter="/"
        )
        common_prefixes = list(result.search("CommonPrefixes"))
        if common_prefixes == [None]:
            results.append({prefix: list(result.search("Contents"))})
        else:
            for common_prefix in common_prefixes:
                results.extend(
                    self.get_common_prefixes_recursive(
                        common_prefix.get("Prefix")
                    )
                )
        return results

    def convert_results(self, results) -> list:
        """Convert the dictionary from boto to the info we want"""
        new_results = []
        # We won't consider files that are already within 90%
        # of our threshold size
        existing_file_threshold = FILE_SIZE_BYTES * 0.9

        for result in results:
            for key, values in result.items():
                key_list = []
                total_file_size = 0
                for value in values:
                    file_size = value.get("Size")
                    if file_size >= existing_file_threshold:
                        continue
                    key_list.append(self.path_prefix + value.get("Key"))
                    total_file_size += file_size
                new_results.append(
                    {self.path_prefix + key: (key_list, total_file_size)}
                )
        return new_results

    def determine_file_splits(self, result) -> list:
        """Return a list of lists."""
        split_list = []
        key_list = result[0]
        file_size = result[1]
        num_current_files = len(key_list)
        num_compacted_files = math.ceil(file_size / FILE_SIZE_BYTES)
        if num_compacted_files == 0:
            num_compacted_files = 1

        split_size = math.ceil(num_current_files / num_compacted_files)
        if split_size >= 2:
            msg = (
                f"Compacting from {num_current_files} "
                f"to {num_compacted_files} "
                f"files using {split_size} files per compaction."
            )
            LOG.info(msg)
            for i in range(0, len(key_list), split_size):
                split_list.append(key_list[i : i + split_size])  # noqa: E203
        else:
            msg = (
                f"Existing files: {key_list} are already approx. "
                f"{file_size * pow(2, -30) / num_current_files} GB each."
            )
            LOG.info(msg)
            LOG.info("Skipping compaction.")
        return split_list

    def merge_files_in_dataframe(
        self, s3_path, file_name, file_number, file_list
    ) -> None:
        """Return a Pandas DataFrame with merged data from multiple files"""
        df = wr.s3.read_parquet(path=file_list, boto3_session=self.session)

        file_path = f"{s3_path}{file_name}_{file_number}.parquet"
        try:
            msg = f"Writing file {file_path}"
            LOG.info(msg)
            wr.s3.to_parquet(
                df=df,
                path=file_path,
                compression="snappy",
                dataset=False,
                boto3_session=self.session,
            )
            return True
        except (ArrowException, EmptyDataFrame) as err:
            msg = f"Failed to merge parquet files at {s3_path}."
            LOG.warning(msg)
            LOG.warning(err)
            return False

    def remove_uncompacted_files(self, file_list) -> None:
        """Remove the original small files that have been compacted."""
        msg = f"Deleting files: {file_list}"
        LOG.info(msg)
        wr.s3.delete_objects(file_list, boto3_session=self.session)

    def determine_base_file_name(self, path) -> str:
        """Return a base file name based on the path"""
        try:
            base_file_name = path.split("source=")[1].split("/")[0]
        except IndexError:
            base_file_name = "data"
        msg = f"Using base file name: {base_file_name}"
        LOG.info(msg)
        return base_file_name

    def compact(self) -> None:
        """Crawl the S3 bucket and compact parquet files."""
        account_level_prefixes = self.get_common_prefixes(self.data_prefix)

        for prefix in account_level_prefixes:
            msg = f"Handling prefix: {prefix}"
            LOG.info(msg)
            results = self.get_common_prefixes_recursive(prefix)
            results = self.convert_results(results)
            for result in results:
                for path, file_tuple in result.items():
                    msg = f"Determing file compaction for {path}"
                    LOG.info(msg)
                    file_splits = self.determine_file_splits(file_tuple)
                    if file_splits:
                        base_file_name = self.determine_base_file_name(path)
                        for i, file_split in enumerate(file_splits):
                            success = self.merge_files_in_dataframe(
                                path, base_file_name, i, file_split
                            )
                            if success:
                                self.remove_uncompacted_files(file_split)
