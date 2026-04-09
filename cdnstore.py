import os
from pathlib import Path
import asyncio
import aioboto3
from s3fs import S3FileSystem
from botocore.client import Config

def _ext_to_dir(ext: str) -> str:
    if ext in ['png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff', 'webp']: return "images"
    elif ext in ['txt', 'md', 'markdown', 'html', 'htm']: return "articles"
    else: return "files"

class CDNStore:
    def __init__(self, bucket: str, public_access_url_template: str = None):          
        self.bucket = bucket.removeprefix("s3://").removesuffix("/")

        region = os.getenv("S3_REGION")
        endpoint_url = os.getenv("S3_ENDPOINT")
        self.s3_link = S3FileSystem(
            endpoint_url=endpoint_url,
            key=os.getenv("S3_ACCESS_KEY_ID"), 
            secret=os.getenv("S3_SECRET_ACCESS_KEY"), 
            client_kwargs={"region_name": region} if region else None,
            config_kwargs={"s3": {"addressing_style": "virtual"}}
        )
        self.public_url_template = public_access_url_template.rstrip("/") if public_access_url_template else endpoint_url.rstrip("/")+"/{bucket}/{key}"

    def upload_binary(self, path: str, data: bytes) -> str:   
        with self.s3_link.open(f"{self.bucket}/{path.lstrip('/')}", "wb") as f:
            f.write(data)
        return _public_url(self.public_url_template, self.bucket, path)
    
    def upload_text(self, path: str, content: str) -> str:
        with self.s3_link.open(f"{self.bucket}/{path.lstrip('/')}", "w", encoding='utf-8') as f:
            f.write(content)
        return _public_url(self.public_url_template, self.bucket, path)

_CONFIG = Config(s3={'addressing_style': 'virtual'})
_MAX_CONCURRENCY = 100

class AsyncCDNStore:
    def __init__(self, bucket: str, public_access_url_template: str = None, max_concurrency: int = _MAX_CONCURRENCY):
        self.bucket = bucket.removeprefix("s3://").removesuffix("/")
        self.session = aioboto3.Session(
            aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY"),
            region_name=os.getenv("S3_REGION")
        )
        self.endpoint_url = os.getenv("S3_ENDPOINT")
        self.public_url_template = public_access_url_template.rstrip("/") if public_access_url_template else self.endpoint_url.rstrip("/")+"/{bucket}/{key}"
        self.throttle = asyncio.Semaphore(max_concurrency) 

    async def _upload(self, s3_client, key: str, content: bytes) -> str:
        async with self.throttle:
            await s3_client.put_object(
                Bucket=self.bucket, 
                Key=key, 
                Body=content.encode('utf-8'), 
                ContentType="text/plain; charset=utf-8"
            )
        return _public_url(self.public_url_template, self.bucket, key)

    async def upload_text(self, path: str, content: str) -> str:
        """Uploads a single text file. 
        Parameters:
            path should be in the format 'folder/file_name.ext'.
            content is the text content to be uploaded.
        """
        async with self.session.client('s3', endpoint_url=self.endpoint_url, config=_CONFIG) as s3:
            return await self._upload(s3, path, content)

    async def batch_upload_texts(self, data: list[dict]) -> dict[str, str]:
        """Uploads multiple text items concurrently. 
        Parameters:
            data: A list of dictionaries, each containing 'path' and 'content' keys.
                'path' should be in the format 'folder/file_name.ext'.
                'content' is the text content to be uploaded.
        """
        async with self.session.client('s3', endpoint_url=self.endpoint_url, config=_CONFIG) as s3:
            return await asyncio.gather(*(self._upload(s3, item['path'], item['content']) for item in data))

def _parse_s3_path(file_path: str) -> tuple[str, str]:
    """Parses bucket/folder/file_name.ext into (bucket, folder/file_name.ext)"""
    return tuple(file_path.strip().removeprefix("s3://").split('/', 1))        

def _public_url(public_url_template: str, bucket: str, key: str) -> str:
    """Creates a public access URL based on template. Ex: https://{bucket}.t3.tigrisfiles.io/{key}"""
    return public_url_template.format(bucket=bucket, key=key)
        

