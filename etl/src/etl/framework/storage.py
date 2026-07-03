"""MinIO/S3 storage options for delta-rs.

Uses conditional-put (If-None-Match / ETag) for SAFE concurrent commits on MinIO — NOT
AWS_S3_ALLOW_UNSAFE_RENAME (which disables the safety check) and NOT a DynamoDB lock.
"""

from __future__ import annotations

import os


def minio_storage_options() -> dict[str, str]:
    return {
        "AWS_ENDPOINT_URL": os.environ["MINIO_ENDPOINT"],  # full URL incl. scheme + :9000
        "AWS_ACCESS_KEY_ID": os.environ["MINIO_ACCESS_KEY"],
        "AWS_SECRET_ACCESS_KEY": os.environ["MINIO_SECRET_KEY"],
        "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
        "AWS_ALLOW_HTTP": "true",
        "aws_conditional_put": "etag",  # safe concurrent writes on MinIO, no lock provider
    }
