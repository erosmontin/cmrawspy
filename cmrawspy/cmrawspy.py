import json
import os
import requests
import boto3
from pynico_eros_montin import pynico as pn

def s3FileTolocal(J, s3=None, pt="/tmp"):
    key = J["key"]
    bucket = J["bucket"]
    filename = J["filename"]
    O = pn.Pathable(pt)
    O.addBaseName(filename)
    O.changeFileNameRandom()
    f = O.getPosition()
    downloadFileFromS3(bucket,key,f, s3)
    J["filename"] = f
    J["type"] = "local"
    return J

def getBucketAndKeyIdFromUplaodEvent(event):
    """
    Args:
        event (_type_): _description_
    Returns:
        bucket_name (str): bucket name
        file_key (str): file key identifier
    """
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    file_key = event["Records"][0]["s3"]["object"]["key"]
    return bucket_name, file_key

def downloadFileFromS3(bucket_name,file_key,outfile=None, s3=None):
    """
    Args:
        bucket_name (str): bucket name
        file_key (str): file key identifier
        outfile (str): output file name
        s3 (_type_): the s3 resource
    Returns:
        filename (str): output file name
    """
    if s3 == None:
        s3 = boto3.resource("s3")
    if out_file == None:
        out_file = pn.createRandomTemporaryPathableFromFileName("a.json")
    s3.Bucket(bucket_name).download_file(file_key, outfile)
    return outfile

def getFileLocal(filedict):
    """    
    Args:
        s (dict): {
                "type": "file",
                "id": -1,
                "options": {
                    "type": "local",
                    "filename": "/data/MYDATA/TESStestData/Density.nii.gz",
                    "options": {}
                }
            }
    Returns:
      fn (str): position of the file in the local filesystem
    """
    s=filedict["options"]
    if (s["type"].lower()=='local'):
        return s["filename"]
    elif (s["type"].lower()=='s3'):
        T=pn.createRandomTemporaryPathableFromFileName(s["filename"])
        T=downloadFileFromS3(s["bucket"],s["key"],T)
        return T
    else:
        raise Exception("I can't get this file modality")

