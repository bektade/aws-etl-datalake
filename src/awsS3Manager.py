import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError


class S3Bucket:
    """S3 Manager class for handling all S3 operations."""

    def __init__(self):
        """Initialize S3 client with credentials from .env file."""
        load_dotenv()

        # Get AWS credentials from .env file
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION')
        self.bucket_name = os.getenv('S3_BUCKET_NAME')

        # Validate credentials
        if not all([self.aws_access_key_id, self.aws_secret_access_key, self.aws_region]):
            raise ValueError(
                "AWS credentials not found in environment variables")

        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region
        )

        print(f"S3 Manager initialized for bucket: {self.bucket_name}")

    def create_bucket(self):
        """Create S3 bucket if it doesn't exist."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"Bucket '{self.bucket_name}' already exists")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                try:
                    if self.aws_region == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={
                                'LocationConstraint': self.aws_region}
                        )
                    print(f"Bucket '{self.bucket_name}' created successfully")
                    return True
                except ClientError as create_error:
                    print(f"Error creating bucket: {create_error}")
                    return False
            else:
                print(f"Error checking bucket: {e}")
                return False

    def create_prefix(self, prefix):
        """Create a prefix (folder) in the S3 bucket."""
        try:
            # Create an empty object with the prefix to establish the "folder"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=f"{prefix}/"
            )
            print(f"Prefix '{prefix}' created in bucket '{self.bucket_name}'")
            return True
        except Exception as e:
            print(f"Error creating prefix: {e}")
            return False

    def upload2Bucket(self, local_file_path, s3_key):
        """Upload a CSV file to S3."""

        # create a bucket if it does not exist
        # same as calling  bucketCreated = s3_manager.create_bucket() ouside class
        # bucket_created = self.create_bucket()

        try:
            with open(local_file_path, 'rb') as f:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=f.read(),
                    ContentType='text/csv'
                )
            print(f"Uploaded '{local_file_path}' to S3 as '{s3_key}'")
            return True
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            return False

    def list_objects(self, prefix=''):
        """List objects in the bucket with optional prefix."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            return response.get('Contents', [])
        except Exception as e:
            print(f"Error listing objects: {e}")
            return []
