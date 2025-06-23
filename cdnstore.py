import boto3
import random
from datetime import datetime

IMAGES_FOLDER = "images"
ARTICLES_FOLDER = "articles"

class CDNStore:
    endpoint = None
    _client = None
    
    def __init__(self, endpoint: str, key: str, secret: str):
        """Initialize store with credentials
        
        Args:
            endpoint: S3 endpoint URL including the container name (e.g. https://espresso-cdn.nyc3.digitaloceanspaces.com)
            container: Bucket/container name
            key: Access key
            secret: Secret key
        """
        self.endpoint = endpoint
        self.key = key
        self.secret = secret       

    @property
    def client(self):
        if not self._client:
            self._client = boto3.client(
                's3',
                endpoint_url=self.endpoint,
                aws_access_key_id=self.key,
                aws_secret_access_key=self.secret
            )
        return self._client

    def upload_article(self, data: str, blob_name: str = None) -> str:
        """Upload article synchronously
        
        Args:
            data: Article text content
            blob_name: Name of the file
            
        Returns:
            Public URL of the uploaded file
        """
        data = data.encode('utf-8')
        blob_name = blob_name or f"{int(datetime.now().timestamp())}-{random.randint(1000, 9999)}.txt"
        self.client.put_object(
            Bucket=ARTICLES_FOLDER,
            Key=blob_name,
            Body=data,
            ACL='public-read',
            ContentType='text/markdown'
        )
        return f"{self.endpoint}/{ARTICLES_FOLDER}/{blob_name}"
        
    def upload_image(self, data: bytes, blob_name: str = None) -> str:
        """Upload image synchronously
        
        Args:
            data: binary image content
            blob_name: Name of the file
            
        Returns:
            Public URL of the uploaded file
        """
        blob_name = blob_name or f"{int(datetime.now().timestamp())}-{random.randint(1000, 9999)}.png"
        self.client.put_object(
            Bucket=IMAGES_FOLDER,
            Key=blob_name,
            Body=data,
            ACL='public-read',
            ContentType='image/png'
        )
        return f"{self.endpoint}/{IMAGES_FOLDER}/{blob_name}"
