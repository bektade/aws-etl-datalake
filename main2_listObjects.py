from src.Ingestor import Ingestor
from s3_manager import S3Bucket
import os


def main2():

    try:

        # create s3 object & initalize
        s3_manager = S3Bucket()

        # list objects in the bucket
        print("\n3. Listing objects in the S3 bucket:")
        objs = s3_manager.list_objects(prefix='BRONZE/')
        # print(objs)

        for x in objs:
            print(x)

        print("\n\n")
        for x in objs:
            print("Key : ", x['Key'])
            print("LastModified : ", x['LastModified'])
            print("ChecksumAlgorithm : ", x['ChecksumAlgorithm'])

    except Exception as e:
        print(f"\n Object listing failed: {e}")
        return False


if __name__ == "__main__":
    success = main2()
    if not success:
        exit(1)
