"""Модуль для работы с S3"""

from src.s3.client import S3Client
from src.s3.scanner import S3Scanner

__all__ = ["S3Client", "S3Scanner"]
