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
TAR_GZ_FILE_IDS = [
'1kbEvDL_UOaWU3F3oqMZLRmCWGWHSSUBd',
'1dfzUv1W4_SFgFgOB19iC9KeYQq5GehZZ',
'1HiaSm-nrNUR3vZiYQYpjBlSVnnx5qZFu',
'1TgAzrfl3mw9EYskxaIATbE2oQ2gzB05v',
'1ZduJvOT_9vDZjjp4Rq64cYeZRaMGKRRT',
'1H97pmYjNRZ1AWLABg-wVXmyvJX0RRd9b',
'1QuuJJYy-xmSMAUPVC-32Mbq00xb1gms1',
'1KeIjaLBY-h-2iVQ_osvspa3dhfcAfPvA',
'1q4WY7iqwyzDXaXWgJdpWKEva1ZcEXPJE',
'123cpewD5Ha0YtKthsFWXVCrQWHIhkELO',
'1T5xMtYm6MF-NWhftKMStob4LUWjbK0UU',
'1D9ulSpKIhTIMeHKdhcvOT8tcjrrZ7fEe',
'1VlxsdfhH6N8dEuOgf2crkQEn40298CKU',
'1h7pG7xGmH9FtmWAam2fn9BrAiUXaVZPC',
'1Og_Rez6qcOqkyuOPnU_WTBF07KDov3ya',
'1vMpfHgTqtLbP02Xfo6P_pQ9jpgi7RJ0m',
'1xEi-RjXWc5AsNQb8DvkWYBmfSNuPmQ99',
'1EWm7Sq-Q7It9a4yulO4-GluHq0qsJxww',
'1qluxPfbfmOTFYdNPuraNEU9WGVNEOdEB',
'1CJcpSppBeEmriCUwah-rs9869Zd7bhMh',
'1kyhLkSvBSH0iSWpfhUHk4EWFV39_Nd1G',
'1vPT2R9EBkEq6npYISSgKFTU8YkVk2zMS',
'1T3HI1QgKmqvwWiDvs6rqjVtOf3JjXC_P',
'1HCR5Eoi9t9OvyZC3LTd12rFT5VJfV57f',
'156x5t-ZCIvTZwwdM-Zlq5SpZ5TmThmVj',
'11xNj-0umBhXS8HaIv5y5f7Af5dESZsh4',
'1GdfjbkF_TM083xRqj7J6ABY12mSKXRfD',
'1VIEKbbVlemZ_Wq3wkUcysmt-Csln4A5Y',
'1Wi5HfzZEtMVojt2XfeIzqDkgfjfY6mUc',
'1z3f61VEPDbiNPA4RjYt427gzJcROVfwN',
'1bnbJHzlq40w2TpRlgUlW5U3w80h3XwaQ',
'1-dPyyjDw5deH4M4prcJMXr192J245xnM',
'1nfTp86usSAPVv9e-tE6ExngdbOJMprRc',
'1swNaDE6In2tAex_fnLl9TVoOrnIYPCmz',
'1lkbHF_xismE2oKWYJhd4Y5VJdN8ZvuKX',
'1PHZfSbGlJ74WWYExkXXMWLNtUjHHsgks',
'1WMTx2ElIseB_OmpARB3Izy49WvrkNvI9',
'18ZGfxztRbLzIhBP2oSXhw-OWT9BXFLjn',
'1wK3KRwiATxo0WXnNM_0obm-nwJnRS7sf',
'14gM40QoA0gqzjABSsqsY09lqjx8t999t']

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
