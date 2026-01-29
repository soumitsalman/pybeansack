import os
from .utils import random_filename

def _ext_to_dir(ext: str) -> str:
    if ext in ['png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff', 'webp']: return "images"
    elif ext in ['txt', 'md', 'markdown', 'html', 'htm']: return "articles"
    else: return "files"

class CDNStore:
    """Lightweight S3 store using s3fs.

    The store exposes simple helpers: upload_bytes and upload_file. It constructs
    object keys using the parsed bucket prefix and ensures objects are saved
    under the 'articles' folder by default (per your request images go to that
    folder as well).
    """
    fs = None
    root_path: str = None
    cdn_url: str = None

    def __init__(
        self,
        root_path: str,        
        cdn_url: str = None
    ):  
        is_s3 = root_path.startswith("s3://")
        self.root_path = root_path.removeprefix("s3://").removesuffix('/')
        if cdn_url: self.cdn_url = cdn_url.rstrip('/')

        if is_s3:
            from s3fs import S3FileSystem
            region = os.getenv("S3_REGION")
            self.fs = S3FileSystem(
                endpoint_url=os.getenv("S3_ENDPOINT_URL"),
                key=os.getenv("S3_ACCESS_KEY_ID"), 
                secret=os.getenv("S3_SECRET_ACCESS_KEY"), 
                client_kwargs={"region_name": region} if region else None,
                config_kwargs={"s3": {"addressing_style": "virtual"}}
            ) 
        # else:
        #     self.fs = os # use local filesystem

    def _relative_path(self, blob_name: str, ext: str) -> str:
        """Return s3fs-style path: 'bucket/key'"""
        blob_name = blob_name or random_filename(ext)
        return f"{self.root_path}/{_ext_to_dir(ext)}/{blob_name}.{ext}"

    def _public_url(self, key: str) -> str:
        return f"{self.cdn_url}/{key}"

    def upload_file(self, source_file_path: str, directory: str = None) -> str:
        from pathlib import Path
        file_name = Path(source_file_path).name
        file_path = f"{directory}/{file_name}" if directory else file_name
        self.fs.put(source_file_path, f"{self.root_path}/{file_path}")
        return self._public_url(file_path)
    
    def upload_binary(self, data: bytes, file_path: str = None) -> str:
        with self.fs.open(f"{self.root_path}/{file_path}", "wb") as f:
            f.write(data)
        return self._public_url(file_path)
    
    def upload_text(self, data: str, file_path: str = None) -> str:
        with self.fs.open(f"{self.root_path}/{file_path}", "w", encoding='utf-8') as f:
            print(f.write(data))
        return self._public_url(file_path)

