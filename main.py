import os
import shutil
import tarfile
import boto3
import subprocess
from pathlib import Path
from tqdm import tqdm
from botocore.client import Config

# S3 Configuration
S3_BUCKET = 'test4'
S3_ENDPOINT_URL = "https://s3.hpccloud.lngs.infn.it"
AWS_ACCESS_KEY_ID = "EQKLIS2F7788CV090CPT"
AWS_SECRET_ACCESS_KEY = "UTCw4Ebpi7a2n1Mn28ET0p2dDye3WHIFkFSyBVn7"

# Paths
LOCAL_DOWNLOAD_DIR = Path("./data")
LOCAL_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Replace with actual file IDs from Google Drive
TAR_GZ_FILE_IDS = ['1kbEvDL_UOaWU3F3oqMZLRmCWGWHSSUBd']

# Setup S3 client
s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    # region_name="us-east-1",  # Required even for MinIO
    # config=Config(signature_version='s3v4')
)

def decompress_tar_gz(src_path: Path, dest_dir: Path = None) -> Path:
    if dest_dir is None:
        dest_dir = src_path.parent / src_path.stem.replace(".tar", "")
    dest_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(src_path, "r:gz") as tar:
        tar.extractall(path=dest_dir)
    return dest_dir

def upload_to_s3(file_path: Path, key_prefix: str = ""):
    key = f"{key_prefix}{file_path.name}".replace("\\", "/")  # Use only the file name
    try:
        s3.upload_file(str(file_path), S3_BUCKET, key)
    except Exception as e:
        print(f"⚠️ Failed to upload {file_path} to {S3_BUCKET}/{key}: {e}")


def clean_up(paths):
    for p in paths:
        try:
            if p.is_file():
                p.unlink()
            elif p.is_dir():
                shutil.rmtree(p)
        except Exception as e:
            print(f"⚠️ Failed to delete {p}: {e}")

def download_file(file_id: str, destination: Path) -> Path:
    try:
        subprocess.run([
            "gdown", f"https://drive.google.com/uc?id={file_id}",
            "-O", str(destination)
        ], check=True)
        return destination
    except Exception as e:
        print(f"❌ Failed to download file ID {file_id}: {e}")
        return None

def main():
    for file_id in tqdm(TAR_GZ_FILE_IDS, desc="📥 Downloading and processing"):
        try:
            tar_path = LOCAL_DOWNLOAD_DIR / f"{file_id}.tar.gz"
            downloaded = download_file(file_id, tar_path)

            if downloaded and downloaded.exists():
                extracted_dir = decompress_tar_gz(downloaded)
                for file in extracted_dir.rglob("*.*"):
                    if file.is_file():
                        upload_to_s3(file)
                clean_up([downloaded, extracted_dir])
        except Exception as e:
            print(f"⚠️ Error processing file ID {file_id}: {e}")

if __name__ == "__main__":
    main()
