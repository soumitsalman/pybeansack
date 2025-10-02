import os
import random
from datetime import datetime
import s3fs


_random_blob_name = lambda ext: f"{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}-{random.randint(1000, 9999)}"
def _ext_to_dir(ext: str) -> str:
    if ext in ['png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff', 'webp']: return "images"
    elif ext in ['txt', 'md', 'markdown', 'html', 'htm']: return "articles"
    else: return "files"

class S3Store:
    """Lightweight S3 store using s3fs.

    The store exposes simple helpers: upload_bytes and upload_file. It constructs
    object keys using the parsed bucket prefix and ensures objects are saved
    under the 'articles' folder by default (per your request images go to that
    folder as well).
    """

    def __init__(
        self,
        endpoint_url: str,
        access_key_id: str,
        secret_key: str,
        bucket: str,
        region: str = None,
        public_url: str = None
    ):      
        self.endpoint_url = endpoint_url
        self.bucket = bucket  
        self.fs = s3fs.S3FileSystem(
            endpoint_url=endpoint_url,
            key=access_key_id, 
            secret=secret_key, 
            client_kwargs={"region_name": region} if region else None
        )     
        self.public_url = (public_url or endpoint_url).rstrip('/')   

    def _relative_path(self, blob_name: str, ext: str) -> str:
        """Return s3fs-style path: 'bucket/key'"""
        blob_name = blob_name or _random_blob_name(ext)
        return f"{self.bucket}/{_ext_to_dir(ext)}/{blob_name}.{ext}"

    def _public_url(self, key: str) -> str:
        return f"{self.public_url}/{key}"

    def upload_bytes(self, data: bytes, blob_name: str = None, ext: str = "png") -> str:
        """Upload raw bytes into '.../articles/<blob_name>' and return public URL."""
        file_key = self._relative_path(blob_name, ext)
        mode = "wb" if "/images/" in file_key else "w"
        data = data if mode == "wb" else str(data.decode('utf-8'))
        with self.fs.open(file_key, mode) as f:
            f.write(data)
        return self._public_url(file_key)
    
    def upload_file(self, file_path: str) -> str:
        """Upload a local file into '.../articles/<blob_name>' and return public URL."""
        from icecream import ic
        file_key = self._relative_path(
            os.path.splitext(os.path.basename(file_path))[0], 
            os.path.splitext(file_path)[1].lstrip('.') or 'png'
        )
        self.fs.put(file_path, file_key)
        return self._public_url(file_key)
    
    def upload_image(self, data: bytes, blob_name: str = None) -> str:
        """Upload image synchronously to ./images/"""
        return self.upload_bytes(data, blob_name, ext='png')
    
    def upload_article(self, data: str, blob_name: str = None) -> str:
        """Upload article synchronously t ./articles/"""
        return self.upload_bytes(data.encode('utf-8'), blob_name, ext='html')

