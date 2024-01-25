# cmaraws
My package for managing aws from [cmr]("https://cloudmrhub.com")

# Installation
```
pip install git+https://github.com/erosmontin/cmrawspy

```

# usage
```
    from cmrawspy import cmrawspy as cm

    a=ima.Imaginable()
    b=ima.Imaginable()
    a.setImageFromNumpy(np.random.random((10,10,10)))
    b.setImageFromNumpy(np.random.random((10,10,10)))



    R=cmrOutput("TESS","/g/zzz.zip",'/g/aaa/')
    # Create a session
    # read aws credentials from a file
    AWS_ACCESS_KEY, AWS_SECRET_KEY,AWS_SESSION_TOKEN = getAwsCredentials('/home/eros/.aws/credentials')

    s3=getS3Resource(AWS_ACCESS_KEY, AWS_SECRET_KEY,AWS_SESSION_TOKEN)
    

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
    # R.exportAndZipResults(delete=True)
    R.exportAndZipResultsToS3(delete=True,s3=s3,bucket="mytestcmr")

```

[*Dr. Eros Montin, PhD*](https://me.biodimensional.com)
**46&2 just ahead of me!**

