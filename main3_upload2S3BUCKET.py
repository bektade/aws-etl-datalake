from src.Ingestor import Ingestor
from src.s3_manager import S3Manager
import os


def main3():

    try:
        # Step: Upload to S3
        print("\n2. Uploading to S3...")

        # create s3 object & initalize
        s3_manager = S3Manager()

        # create s3 bucket if it doesn't exist
        bucketCreated = s3_manager.create_bucket()
        print(f"Bucket created: {bucketCreated}")

        csvPath = 'new/oct_20251031_000836.csv'

        filename = os.path.basename(csvPath)

        # PREFIX/FileName
        s3_key = f"BRONZE/{filename}"

        if s3_manager.upload2Bucket(csvPath, s3_key):
            print(f"Uploaded to S3: s3://{s3_manager.bucket_name}/{s3_key}")
        else:
            print("Failed to upload to S3")
            return False

    except Exception as e:
        print(f"\Upload failed: {e}")
        return False


if __name__ == "__main__":
    success = main3()
    if not success:
        exit(1)
