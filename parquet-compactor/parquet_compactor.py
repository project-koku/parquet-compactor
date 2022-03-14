import datetime
import gc
import logging
from collections import defaultdict
from functools import cached_property

import awswrangler as wr
import boto3
import environ
from awswrangler.exceptions import EmptyDataFrame
from pyarrow import ArrowException


LOG = logging.getLogger(__name__)

TARGET_FILE_SIZE_GB = environ.Env().get_value(
    "TARGET_FILE_SIZE_GB", cast=float, default=0.3
)
FILE_SIZE_BYTES = TARGET_FILE_SIZE_GB * pow(2, 30)

SKIP_SOURCE_TYPE_CURRENT_MONTH = (
    environ.Env()
    .get_value("SKIP_SOURCE_TYPE_CURRENT_MONTH", default="AWS,Azure")
    .split(",")
)


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

    @cached_property
    def current_year_str(self):
        """Return the current year as a string."""
        return datetime.datetime.utcnow().strftime("%Y")

    @cached_property
    def current_month_str(self):
        """Return the current month as a string."""
        return datetime.datetime.utcnow().strftime("%m")

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

        for result in results:
            for key, values in result.items():
                file_keys = []
                for value in values:
                    file_size = value.get("Size")
                    if file_size >= FILE_SIZE_BYTES:
                        continue
                    file_keys.append(
                        (self.path_prefix + value.get("Key"), file_size)
                    )
                new_results.append({self.path_prefix + key: file_keys})
        return new_results

    def determine_file_splits(self, file_tuples) -> list:
        """Return a list of lists."""
        split_dict = defaultdict(list)
        size_dict = defaultdict(float)

        split_count = 1
        # Loop through our list of files in this S3 "directory"
        for file_tuple in file_tuples:
            file_key = file_tuple[0]
            file_size = file_tuple[1]
            file_inserted = False
            # See if this file can be combined with another
            for i in range(split_count):
                if size_dict[i] + file_size <= FILE_SIZE_BYTES:
                    split_dict[i].append(file_key)
                    size_dict[i] += file_size
                    file_inserted = True
            if not file_inserted:
                # Insert the file into a new bin
                split_dict[split_count].append(file_key)
                size_dict[split_count] += file_size
                split_count += 1

        return [
            file_list
            for file_list in split_dict.values()
            if len(file_list) > 1
        ]

    def merge_files_in_dataframe(
        self, s3_path, file_name, file_number, file_list
    ) -> None:
        """Return a Pandas DataFrame with merged data from multiple files"""
        success = False
        msg = f"Reading {file_list} from S3."
        LOG.info(msg)
        df = wr.s3.read_parquet(path=file_list, boto3_session=self.session)
        file_path = f"{s3_path}{file_name}_{file_number}.parquet"
        try:
            msg = f"Combining files. Writing file {file_path}"
            LOG.info(msg)
            wr.s3.to_parquet(
                df=df,
                path=file_path,
                compression="snappy",
                dataset=False,
                boto3_session=self.session,
            )

            success = True
        except (ArrowException, EmptyDataFrame) as err:
            msg = f"Failed to merge parquet files at {s3_path}."
            LOG.warning(msg)
            LOG.warning(err)
            success = False
        finally:
            # Force a free up on memory
            del df
            gc.collect()
        return success

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

    def should_skip_compacting(self, path):
        """Determine if we should skip compacting these files.

        Because AWS and Azure data are overwritten during the current month,
        the compacted files would be deleted on the next processing run.
        """
        is_current_month_data = (
            f"year={self.current_year_str}" in path
            and f"month={self.current_month_str}" in path
        )
        is_skippable_source_type = any(
            [
                source_type in path
                for source_type in SKIP_SOURCE_TYPE_CURRENT_MONTH
            ]
        )

        return (
            True
            if is_current_month_data and is_skippable_source_type
            else False
        )

    def compact(self) -> None:
        """Crawl the S3 bucket and compact parquet files."""
        account_level_prefixes = self.get_common_prefixes(self.data_prefix)

        for prefix in account_level_prefixes:
            msg = f"Handling prefix: {prefix}"
            LOG.info(msg)
            results = self.get_common_prefixes_recursive(prefix)
            results = self.convert_results(results)
            for result in results:
                for path, file_tuples in result.items():
                    if self.should_skip_compacting(path):
                        msg = f"Skipping compacting for {path}."
                        LOG.info(msg)
                        continue
                    msg = f"Determing file compaction for {path}"
                    LOG.info(msg)
                    file_splits = self.determine_file_splits(file_tuples)
                    if file_splits:
                        base_file_name = self.determine_base_file_name(path)
                        for i, file_split in enumerate(file_splits):
                            success = self.merge_files_in_dataframe(
                                path, base_file_name, i, file_split
                            )
                            if success:
                                self.remove_uncompacted_files(file_split)
                    else:
                        LOG.info("No files to compact. Skipping compaction.")
