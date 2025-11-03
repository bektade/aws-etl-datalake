from src.Ingestor import Ingestor
from src.awsS3Manager import S3Bucket
import os


def main():

    try:
        # Step 1: Ingest data from API and save to CSV
        print("\n1. Ingesting data from API...")

        # init Ingestor with crime end point
        ingestor = Ingestor(endpoint='ijzp-q8t2.json',
                            base_url="https://data.cityofchicago.org/resource")

        # fetch from api gets a df
        df_ = ingestor.fetchApi(pastDays=10, columns=None)

        # save file locally

        csvPath = ingestor.saveCSV(
            df_, path='RawBronze/DataSet1', filePrefix='oct')

        # Step 2: Upload to S3
        print("\n2. Uploading to S3...")

        # create s3 object & initalize
        s3buck = S3Bucket()

        # create s3 bucket if it doesn't exist
        bucketCreated = s3buck.create_bucket()
        print(f"Bucket created: {bucketCreated}")

        filename = os.path.basename(csvPath)

        # PREFIX/FileName
        s3_key = f"BRONZE/{filename}"

        if s3buck.upload2Bucket(csvPath, s3_key):
            print(f"✅ Uploaded to S3: s3://{s3buck.bucket_name}/{s3_key}")
        else:
            print("❌ Failed to upload to S3")
            return False

        # list objects in the bucket
        print("\n3. Listing objects in the S3 bucket:")
        objs = s3buck.list_objects(prefix='BRONZE/')
        print(objs)

        print("\n" + "=" * 50)
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 50)
        return True

    except Exception as e:
        print(f"\nPipeline failed: {e}")
        return False


if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)
