import os
from pathlib import Path
from minio import Minio
from minio.error import S3Error


class MinioClient:
    def __init__(
        self,
        endpoint: str = None,
        access_key: str = None,
        secret_key: str = None,
        secure: bool = False,
    ):
        """Initializes the MinIO client. Defaults to standard local Docker environment variables."""
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
                print(f"Bucket '{bucket_name}' created successfully.")
            else:
                print(f"Bucket '{bucket_name}' already exists.")
        except S3Error as e:
            print(f"Error checking/creating bucket: {e}")
            raise

    def upload_file(self, local_path: str, bucket_name: str, object_name: str) -> None:
        """Uploads a single local file to a specific MinIO path."""
        self.create_bucket_if_not_exists(bucket_name)
        try:
            self.client.fput_object(
                bucket_name=bucket_name, object_name=object_name, file_path=local_path
            )
            print(
                f"Successfully uploaded {local_path} to s3://{bucket_name}/{object_name}"
            )
        except S3Error as e:
            print(f"Failed to upload {local_path}: {e}")
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
