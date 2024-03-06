import boto3
import csv
import time
import json
import os
from ec2_metadata import ec2_metadata
from hashlib import md5

ROOT = "/home/ec2-user/"
BUCKET = "zeev-blender"

sqs = boto3.resource('sqs')

#s3 = boto3.resource('s3')
s3 = boto3.client('s3')

queue = sqs.get_queue_by_name(QueueName='zeev-blender.fifo')

status_queue = sqs.get_queue_by_name(QueueName='zeev-blender-status.fifo')

lastStatusTimestamp = 0

print(queue)
print(status_queue)


def factor_of_1MB(filesize, num_parts):
    x = filesize / int(num_parts)
    y = x % 1048576
    return int(x + 1048576 - y)

def calc_etag(inputfile, partsize):
    md5_digests = []
    with open(inputfile, 'rb') as f:
        for chunk in iter(lambda: f.read(partsize), b''):
            md5_digests.append(md5(chunk).digest())
    return md5(b''.join(md5_digests)).hexdigest() + '-' + str(len(md5_digests))

def possible_partsizes(filesize, num_parts):
    return lambda partsize: partsize < filesize and (float(filesize) / float(partsize)) <= num_parts


def updateBlenderFile2(blender):
  
    global ROOT
    global BUCKET

    blendFile = ROOT+"blender/"+blender
    etagFile = blendFile+".etag"
    
    etag = s3.head_object(Bucket=BUCKET, Key=blender)["ETag"].strip('"').strip("'")
    
    if os.path.exists(blendFile) and os.path.exists(etagFile):
    
        with open(etagFile) as f:
            etagString = f.read()
        
        print(etagString, " == ", etag)
        
        if etagString == etag:
            print('Local file matches. no need to update')
            return
            
    print("downloading blend file", blender)
    s3.download_file(BUCKET, blender, blendFile)
    
    with open(etagFile, 'w') as f:
        f.write(etag)
    
            


def updateBlenderFile(blender):

    global ROOT
    global BUCKET

    blendFile = ROOT+"blender/"+blender

    if os.path.exists(blendFile):

        etag = s3.head_object(Bucket=BUCKET, Key=blender)["ETag"].strip('"').strip("'")

        filesize  = os.path.getsize(blendFile)
        num_parts = int(etag.split('-')[1])

        partsizes = [ ## Default Partsizes Map
            8388608, # aws_cli/boto3
            15728640, # s3cmd
            factor_of_1MB(filesize, num_parts) # Used by many clients to upload large files
        ]

        for partsize in filter(possible_partsizes(filesize, num_parts), partsizes):
            if etag == calc_etag(blendFile, partsize):
                print('Local file matches. no need to update')
                return

    print("downloading blend file", blender)
    s3.download_file(BUCKET, blender, blendFile)


def updateStatus(status, body={}):

    global lastStatusTimestamp


    if status == "idel" and lastStatusTimestamp > time.time() - 5*60:
        return

    print("sending status", status)
    
    body["instanceID"] = ec2_metadata.instance_id
    body["status"] = status
    body["timestamp"] = time.time()

    response = status_queue.send_message(MessageBody=json.dumps(body), MessageGroupId=ec2_metadata.instance_id)

    lastStatusTimestamp = time.time()
    

def getCacheDirs():

    chacheDirSet = set()
    result = s3.list_objects(Bucket=BUCKET,
    Prefix='blendcache'
    )

    if "Contents" in result:
        for file in result["Contents"]:
            #print(file)
            #print(file["Key"].split("/")[0])
            chacheDirSet.add(file["Key"].split("/")[0])

    result = s3.list_objects(Bucket=BUCKET,
    Prefix='cache'
    )

    if "Contents" in result:
        for file in result["Contents"]:
            #print(file)
            #print(file["Key"].split("/")[0])
            chacheDirSet.add(file["Key"].split("/")[0])

    return chacheDirSet

    
if __name__ == '__main__':




    print ("starting render consumer")
   
    updateStatus("starting up") 


    while True:
    
        
        
        #getJob
        
        messages = queue.receive_messages(MaxNumberOfMessages=1)
       
        if len(messages) <= 0:
            updateStatus("idel")

        for message in messages:
        

            body = json.loads(message.body)
            print (body)
            
            updateStatus("starting job", body)
            
            blender = body["blender"]
            scene = body["scene"]
            frame_end = body["frame_end"]
            frame_start = body["frame_start"]


            configFile = "/home/ec2-user/config.py"
            render_device = "CUDA"
            if "arch" in body.keys():
                if body["arch"] == "CPU":
                    render_device = "CPU"

            if not os.path.isfile(configFile):
                print ("ERROR bad config file %s" % configFile)
                message.delete()

                updateStatus("error: bad config %s" % configFile)
                continue

            
            #first update blender
            #s3.download_file("zeev-blender", body["blender"], "blender/%s" % blender)
            #download_dir("assets", "blender/assets", "zeev-blender")
            updateBlenderFile2(body["blender"])
            os.system("aws s3 sync s3://zeev-blender/assets %s/blender/assets" % ROOT)
            os.system("aws s3 sync s3://zeev-blender/models %s/blender/models" % ROOT)

            for cacheFolder in getCacheDirs():
                os.system("aws s3 sync s3://zeev-blender/%s %s/blender/%s" % (cacheFolder, ROOT, cacheFolder))

            
            os.system("rm %s/render/*" % ROOT)
            #os.system("/home/ec2-user/blenderapp/blender -b /home/ec2-user/blender/%s -S %s -s %i -e %i -E CYCLES -t 0 -o /home/ec2-user/render/#### -P %s -a" % (blender, scene, frame_start, frame_end, configFile))
            print("/home/ec2-user/blenderapp/blender -b /home/ec2-user/blender/%s -S %s -s %i -e %i -E CYCLES -t 0 -o /home/ec2-user/render/#### -y -P %s -a -- --cycles-device %s --cycles-print-stats" % (blender, scene, frame_start, frame_end, configFile, render_device))
            os.system("/home/ec2-user/blenderapp/blender -b /home/ec2-user/blender/%s -S %s -s %i -e %i -E CYCLES -t 0 -o /home/ec2-user/render/#### -y -P %s -a -- --cycles-device %s --cycles-print-stats" % (blender, scene, frame_start, frame_end, configFile, render_device))
        
            updateStatus("uploading", body)

            for filename in os.listdir("%s/render" % ROOT):
                if filename.endswith("jpg") or filename.endswith("png"):
                    response = s3.upload_file("%s/render/%s" % (ROOT,filename), BUCKET, "blender_output/%s/%s" % (scene, filename))
            

            message.delete()
            
            updateStatus("complete")

            print("----------------------------------------------------------------------------------------------------------------------------")
        
        time.sleep(1)

