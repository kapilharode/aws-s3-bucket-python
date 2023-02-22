import boto3
from django.conf import settings
import traceback
import tempfile

class AwsConnectionS3(object):
    """
    AWS S3 connection with S3
    """
    s3_resource_obj =None
    s3_resource = None
    s3_client = None
    folder_name = None

    service_name = settings.SERVICE_NAME
    region_name = settings.REGION_NAME

    aws_access_key_id = settings.AWS_ACCESS_KEY_ID
    aws_secret_access_key = settings.AWS_SECRET_ACCESS_KEY
    aws_storage_bucket_name = settings.AWS_STORAGE_BUCKET_NAME

    def __init__(self, *args, **kwargs):
        """
        initializing the AWS Connection.
        """
        self.folder_name = kwargs.get("folder_name", "generic")
        self.s3_resource = boto3.resource(
            service_name = self.service_name,
            region_name = self.region_name,
            aws_access_key_id = self.aws_access_key_id,
            aws_secret_access_key = self.aws_secret_access_key
        )
        self.s3_client = boto3.client(
            service_name = self.service_name,
            region_name = self.region_name,
            aws_access_key_id = self.aws_access_key_id,
            aws_secret_access_key = self.aws_secret_access_key
        )
        self.s3_resource_obj = self.s3_resource.Bucket(self.aws_storage_bucket_name)

    def get_list_files(self):
        """
        get list of file
        """
        try:
            files_list = []
            for obj in self.s3_resource_obj.objects.filter(Prefix=f"{self.folder_name}/"):
                files_list.append(obj.key)
            return files_list
        except Exception as excep:
            traceback.print_exc()
            return None

    def upload_file(self, file, file_name):
        """
        upload file on aws
        """
        try:
            existing_file_content = file.read()
            temp_file = tempfile.NamedTemporaryFile(mode="wb", delete=True)
            temp_file.write(existing_file_content)
            self.s3_resource_obj.upload_file(Filename=temp_file.name, Key=f"{self.folder_name}/{file_name}")
            temp_file.close()
            return True
        except Exception as excep:
            traceback.print_exc()
            return True

    def get_file_url(self, file_name, expires_time=500):
        """
        generate presigned url
        """
        try:
            file_url = self.s3_client.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': self.aws_storage_bucket_name,
                    'Key': f"{self.folder_name}/{file_name}",
                },
                ExpiresIn=expires_time
            )
            return file_url

        except Exception as excep:
            traceback.print_exc()
            return None

    def delete_file(self, file_name):
        """
        Delete file from Aws S3
        """
        try:
            self.s3_client.delete_object(Bucket=self.aws_storage_bucket_name, Key=f"{self.folder_name}/{file_name}")
            return True
        except Exception as excep:
            traceback.print_exc()
            return None
