import boto3
import re
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import functools
import os

client = boto3.resource('s3')
my_bucket = client.Bucket('cheque-bounce-cases')
base = "../DATA/FINAL_SAMPLE/"

pdfs = ["PDF/","PDF_1/","PDF2/"]

def down(obj):
    path = base + obj.key
    my_bucket.download_file(obj.key, path)
    return

for p in pdfs:
    print(p)
    for obj in my_bucket.objects.filter(Prefix=p):
        print(obj.key)
        my_bucket.download_file(obj.key, obj.key)
