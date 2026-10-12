import os
from pathlib import Path
from minio import Minio
from minio.error import S3Error
from datetime import date
import re
from urllib.parse import urlsplit
from src.core.logger import get_logger
from src.core.config import RUN_MODE
DATE_PARTITION_PATTERN = re.compile(r"^dt=(\d{4}-\d{2}-\d{2})$")
logger = get_logger(__name__)


class MinioClient:
    def __init__(
        self,
        endpoint: str = None,
        access_key: str = None,
        secret_key: str = None,
        secure: bool = False,
        env = RUN_MODE
    ):
        if env == "docker":
            self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "minio:9000")
        else:
            self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")

        self.client = Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=secure,
        )

    def create_bucket_if_not_exists(self, bucket_name: str) -> None:
        """Ensures a bucket exists before operations start."""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                logger.info("Bucket '%s' created successfully.", bucket_name)
            else:
                logger.info("Bucket '%s' already exists.", bucket_name)
        except S3Error:
            logger.exception("Error checking/creating bucket '%s'.", bucket_name)
            raise

    def upload_file(self, local_path: str, bucket_name: str, object_name: str) -> None:
        """Uploads a single local file to a specific MinIO path."""
        self.create_bucket_if_not_exists(bucket_name)
        try:
            self.client.fput_object(
                bucket_name=bucket_name, object_name=object_name, file_path=local_path
            )
            logger.info(
                "Successfully uploaded %s to s3://%s/%s",
                local_path,
                bucket_name,
                object_name,
            )
        except S3Error:
            logger.exception("Failed to upload %s.", local_path)
            raise

    def upload_directory(
        self, local_dir: str, bucket_name: str, target_prefix: str = ""
    ) -> None:
        """
        Recursively uploads an entire folder layout to MinIO.
        """
        self.create_bucket_if_not_exists(bucket_name)
        base_path = Path(local_dir)

        if not base_path.is_dir():
            raise ValueError(f"Provided path is not a directory: {local_dir}")

        for file_path in base_path.rglob("*"):
            if file_path.is_file():
                # Construct object path maintaining folder structure
                relative_path = file_path.relative_to(base_path)
                object_name = os.path.join(target_prefix, str(relative_path)).replace(
                    "\\", "/"
                )

                self.upload_file(str(file_path), bucket_name, object_name)

    def get_max_raw_date(
        self,
        input_path: str = "s3a://warehouse/raw/customers/",
    ) -> date | None:
        """
        Find the latest dt=YYYY-MM-DD partition in MinIO.
        Example:
            source_uri = "s3a://warehouse/raw/customers/"
        Returns:
            date(2026, 5, 5), or None if no matching partitions exist.
        """

        # Parse s3a://warehouse/raw/customers/
        parsed = urlsplit(input_path)

        if parsed.scheme not in {"s3a", "s3"}:
            raise ValueError(f"Expected an s3:// or s3a:// URI, got: {input_path}")

        bucket_name = parsed.netloc
        prefix = parsed.path.strip("/")

        if not bucket_name:
            raise ValueError(f"Missing bucket name: {input_path}")

        # Ensure the prefix ends with "/" when non-empty.
        if prefix:
            prefix += "/"

        logger.info("Checking Raw max date for s3://%s/%s", bucket_name, prefix)
        partition_pattern = re.compile(r"^dt=(\d{4}-\d{2}-\d{2})$")
        max_date = None
        try:
            objects = self.client.list_objects(
                bucket_name=bucket_name,
                prefix=prefix,
                recursive=False,
            )
            for obj in objects:
                object_name = obj.object_name

                # Only inspect immediate children of the source prefix.
                if not object_name.startswith(prefix):
                    continue

                relative_name = object_name[len(prefix) :].strip("/")

                if not relative_name:
                    continue

                # For a directory listing, this should be dt=YYYY-MM-DD.
                partition_name = relative_name.split("/", 1)[0]

                match = partition_pattern.fullmatch(partition_name)

                if not match:
                    continue

                try:
                    partition_date = date.fromisoformat(match.group(1))
                except ValueError:
                    continue

                if max_date is None or partition_date > max_date:
                    max_date = partition_date

            logger.info("Raw max available date: %s", max_date)

            return max_date

        except S3Error:
            logger.exception("Failed to list MinIO partitions.")
            raise

    def list_raw_partition_dates(self, source_uri: str) -> list[date]:
        """
        List actual dt=YYYY-MM-DD partitions beneath a MinIO URI.

        Example:
            s3a://warehouse/raw/customers/

        Returns:
            [date(2026, 5, 1), date(2026, 5, 2), ...]
        """
        parsed = urlsplit(source_uri)

        if parsed.scheme not in {"s3", "s3a"}:
            raise ValueError(f"Expected an s3:// or s3a:// URI: {source_uri}")

        bucket_name = parsed.netloc
        prefix = parsed.path.strip("/")

        if not bucket_name:
            raise ValueError(f"Missing bucket in URI: {source_uri}")

        if prefix:
            prefix += "/"

        logger.info("Listing Raw partitions: s3://%s/%s", bucket_name, prefix)

        partition_dates = set()

        try:
            # Non-recursive listing returns immediate child prefixes.
            objects = self.client.list_objects(
                bucket_name=bucket_name,
                prefix=prefix,
                recursive=False,
            )

            for obj in objects:
                object_name = obj.object_name

                if not object_name.startswith(prefix):
                    continue

                relative_name = object_name[len(prefix) :].strip("/")

                if not relative_name:
                    continue

                # Ignore files directly inside the source prefix.
                partition_name = relative_name.split("/", 1)[0]

                match = DATE_PARTITION_PATTERN.fullmatch(partition_name)

                if not match:
                    continue

                try:
                    partition_date = date.fromisoformat(match.group(1))
                except ValueError:
                    continue

                # With recursive=False, a returned directory prefix
                # is an immediate child such as dt=2026-05-01/.
                # Keep only actual partition prefixes.
                if getattr(obj, "is_dir", False):
                    partition_dates.add(partition_date)

            result = sorted(partition_dates)
            return result

        except S3Error:
            logger.exception(
                "Failed to list Raw partitions at s3://%s/%s", bucket_name, prefix
            )
            raise
