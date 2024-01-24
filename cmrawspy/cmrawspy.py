import uuid
import boto3
from pynico_eros_montin import pynico as pn
import shutil

import scipy

def saveMatlab(fn,vars):
    J=dict()
    for k in vars:
        J[k["name"].replace(" ","")]=k["data"]
    scipy.io.savemat(fn,J)

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

def uploadFiletoS3(filename,bucket_name,file_key=None, s3=None):
    if s3 == None:
        s3 = boto3.resource("s3")
    if file_key == None:
        file_key = str(uuid.uuid4())
    s3.Bucket(bucket_name).upload_file(filename, file_key)
    return {"bucket": bucket_name, "key": file_key}



def getCMRFile(filedict):
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

import pyable_eros_montin.imaginable as ima      
import numpy as np

class cmrOutput:
    def __init__(self,app=None,filename=None,path=None):
            self.out={"headers":{"options":{}},"data":[]}
            if filename!=None:
                self.outputfilename=pn.Pathable(filename)
            else:
                self.outputfilename=pn.createRandomTemporaryPathableFromFileName("a.zip")
            
            self.outputfilename.ensureDirectoryExistence()
            if path==None:
                self.outputpath=pn.createRandomTemporaryPathableFromFileName(self.outputfilename.getPosition()).appendPathRandom()
            else:
                self.outputpath=pn.Pathable(path)
            self.outputpath.ensureDirectoryExistence()
            self.outputfilename.ensureDirectoryExistence()
            self.setApp(app)
            self.forkable=self.outputpath.fork()
            self.forkable.addBaseName("data")
            self.savematlab=True          

    
    def addAbleFromFilename(self,filename,id,name,type="output"):
        L=ima.Imaginable(filename=filename)
        N=pn.Pathable(filename)
        o=self.addAble(L,id,name,type,N.getBaseName())
        o["filename"]=filename
        return o
        
    def addAble(self,L,id,name,type="output",basename=None):
        pixeltype='real'
        im=L.getImageAsNumpy()

        if np.iscomplexobj(im):
            pixeltype='complex'
            L.setImageFromNumpy(im.astype(np.singlecomplex))
        if basename==None:
            basename=pn.createRandomTemporaryPathableFromFileName("a.nii.gz").getBaseName()
        o={'filename':None,
           'basename':basename,
           'able':L,
                'id':id,
                'dim':L.getImageDimension(),
                'name':name,
                'type':type,
                'numpyPixelType':im.dtype.name,
                'pixelType':pixeltype}
        
        self.out["data"].append(o)
        return o
    
    def setHeader(self,a={}):
        for k in a.keys():
            self.out["headers"][k]=a[k]
    def setPipeline(self,a):
        if "options" not in self.out["headers"].keys():
            self.out["headers"]["options"]={}
        self.out["headers"]["options"]["pipeline"]=a

    def setToken(self,a):
        if "options" not in self.out["headers"].keys():
            self.out["headers"]["options"]={}
        self.out["headers"]["options"]["token"]=a
    def setLog(self,a):
        l=self.forkable.fork()
        l.changeBaseName("log.json")
        a.writeLogAs(l.getPosition())
        self.out["log"]=a.getLog()

    def setApp(self,a):
        self.out["app"]=a
    def setTask(self,a):
        l=self.forkable.fork()
        l.changeBaseName("task.json")
        l.writeJson(a)

    
    def setOptions(self,a):
        for k in a.keys():
            self.out["headers"]["options"][k]=a[k]
        l=self.forkable.fork()
        l.changeBaseName("options.json")
        l.writeJson(a)

    def setEvent(self,a):
        l=self.forkable.fork()
        l.changeBaseName("event.json")
        l.writeJson(a)

    def exportResults(self):
        outputdirectory=self.outputpath.getPath()
        #check id the data are in the expected directory
        J=[]
        for d in self.out["data"]:
            theo= outputdirectory+"/data/"+d["basename"]
            pn.Pathable(theo).ensureDirectoryExistence()

            if d["filename"]==None:
                d["able"].writeImageAs(theo)
            
            elif d["filename"]!=theo:
                shutil.copy(d["filename"],theo)
            d["filename"]="data/"+d["basename"]
            
            if self.savematlab:
                J.append({"name":d["name"],"data":d["able"].getImageAsNumpy()})
            if "able" in d.keys():
                del d["able"]
            if "basename" in d.keys():
                del d["basename"]

        #write the json file
        OUT=self.forkable.fork()
        OUT.changeBaseName("info.json")
        OUT.writeJson(self.out)
        OUT.changeBaseName("matlab.mat")
        saveMatlab(OUT.getPosition(),J)

    def changeOutputPath(self,path):
        #copy the data to the new path
        shutil.copytree(self.outputpath.getPosition(),path)
        shutil.rmtree(self.outputpath.getPosition())          
        self.outputpath=pn.Pathable(path)
        self.outputpath.ensureDirectoryExistence()
        self.forkable=self.outputpath.fork()
        self.forkable.addBaseName("data.nii.gz")
        
        return self.outputpath.getPosition()

    def exportAndZipResults(self,outzipfile=None,delete=False):
        p=self.exportResults()
        if outzipfile==None:
            outzipfile=self.outputfilename.getPosition()

        shutil.make_archive(outzipfile[:-4],outzipfile[-3:] , self.outputpath.getPosition())
        if delete:
            shutil.rmtree(self.outputpath.getPosition())
        return outzipfile
    
    def exportAndZipResultsToS3(self,bucket,key=None,outzipfile=None,delete=False,deletezip=False):
        p=self.exportAndZipResults(outzipfile=outzipfile,delete=delete)
        O= uploadFiletoS3(p,bucket,key)
        if deletezip:
            shutil.rmtree(p)
        return O
    


if __name__ == "__main__":
    a=ima.Imaginable()
    b=ima.Imaginable()
    a.setImageFromNumpy(np.random.random((10,10,10)))
    b.setImageFromNumpy(np.random.random((10,10,10)))
    R=cmrOutput("TESS","/g/zzz.zip",'/g/aaa/')
    
    R.addAble(a,1,"test",basename="test.nii.gz")
    R.addAble(a,1,"test2",basename="test2.nii.gz")
    R.addAbleFromFilename("/g/SAR2.nii.gz",3,"test3")
    R.setApp("TESS")
    R.setToken("dede")
    R.setPipeline("dedde")
    L=pn.Log()
    L.append("test")
    L.append("test2")
    R.setLog(L)
    R.changeOutputPath("/g/bbb/")
    R.setTask({"id":1,
    "name":"test"})
    R.setOptions({"id":1,'ded':'rrr'})

    R.changeOutputPath("/g/aa2ea/")
    R.setEvent({"id":1})
    # R.exportAndZipResults(outzipfile='/g/zzz.zip',delete=False)
    R.exportAndZipResults(delete=True)



    


    
    
                

    

            

