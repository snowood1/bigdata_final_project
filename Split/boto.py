import boto3
from botocore.client import Config

ACCESS_KEY_ID = 'AKIAITDWSCTVWHMZLT6Q'
ACCESS_SECRET_KEY = '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'
BUCKET_NAME = 'snowood1'

# for index in range(1,38):
#     filename = 'train-{}.csv'.format(index)
filename = 'C:/Users/Bob Hu/Desktop/BD Proj/train.csv'
print("Name:",filename)
# with open(filename, 'rb') as data:
#     s3 = boto3.resource(
#         's3',
#         aws_access_key_id=ACCESS_KEY_ID,
#         aws_secret_access_key=ACCESS_SECRET_KEY,
#         config=Config(signature_version='s3v4')
#     )
#     s3.Bucket(BUCKET_NAME).put_object(Key= "project/train.csv" , Body=data)
# print ("Done")


# s3 = boto3.resource(
#     's3',
#     aws_access_key_id=ACCESS_KEY_ID,
#     aws_secret_access_key=ACCESS_SECRET_KEY,
#     config=Config(signature_version='s3v4')
# )
#
# s3 = boto3.client('s3')

s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=ACCESS_SECRET_KEY
    # aws_session_token=SESSION_TOKEN,
)
# s3.upload_file(filename, BUCKET_NAME, "project/train.csv", ACCESS_KEY_ID,ExtraArgs={ACCESS_SECRET_KEY})
s3.upload_file(Filename = filename, Bucket=BUCKET_NAME, Key="project/train.csv", ExtraArgs=None, Callback=None, Config=None)
