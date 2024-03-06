import boto3
import sys
import json
import time
import datetime


session = boto3.Session(profile_name='personal', region_name="us-west-2")
sqs = session.resource('sqs')
ec2 = session.resource('ec2')

#print(sqs.list_queues())

queue = sqs.get_queue_by_name(QueueName='zeev-blender.fifo')
status_queue = sqs.get_queue_by_name(QueueName='zeev-blender-status.fifo')

print(queue, status_queue)




if __name__ == '__main__':
    
    if len(sys.argv) < 2:
        print ("USSAGE: render_producer.py list|add|get|deleteall jobID=ID blender=filename scene=scene frame_range=start:end chunks=1 arch=CPU|GPU")
        exit(0)
        
    
    jobID = 0
    blender=""
    scene=""
    frame_range=(0,0)
    chunks = 1
    action = sys.argv[1]
    arch=""
    
    for i in range(2,len(sys.argv)):
        arg = sys.argv[i].split("=")
        if arg[0] == "blender":
            blender = arg[1]
        if arg[0] == "jobID":
            jobID = arg[1]
        if arg[0] == "scene":
            scene = arg[1]
        if arg[0] == "frame_range":
            temp = arg[1].split(":")
            frame_range = (int(temp[0]), int(temp[1]))
        if arg[0] == "chunks":
            chunks = int(arg[1])
        if arg[0] == "arch":
            arch = arg[1]
            
    print (action, jobID, blender, scene, frame_range, arch)

    
    if action == "list":
        
        ApproximateNumberOfMessages = 0
        ApproximateNumberOfMessagesNotVisible = 0
        
        print ("queue status")
        
        servers = {}
        
        
        while True:
        
            
        
            queue.reload()
                
            if ApproximateNumberOfMessages != queue.attributes["ApproximateNumberOfMessages"]:
                ApproximateNumberOfMessages = queue.attributes["ApproximateNumberOfMessages"]
                print ("ApproximateNumberOfMessages", ApproximateNumberOfMessages)
                    
            if ApproximateNumberOfMessagesNotVisible != queue.attributes["ApproximateNumberOfMessagesNotVisible"]:
                ApproximateNumberOfMessagesNotVisible = queue.attributes["ApproximateNumberOfMessagesNotVisible"]
                print ("ApproximateNumberOfMessagesNotVisible", ApproximateNumberOfMessagesNotVisible)
        
            
            
            messages = status_queue.receive_messages(MaxNumberOfMessages=10)
            
            if len(messages) > 0:
                print("\n\n\n\n------------------------------------------------------------------------------------------------\n\n\n\n")
                printUpdate = True
            else:
                printUpdate = False
            
            while len(messages) > 0 and len(messages) < 100:
            
                for message in messages:
                
                    body = json.loads(message.body)
                
                    instanceID = body["instanceID"]
                    
                    if instanceID not in servers.keys():
                        servers[instanceID] = []
                        
                    servers[instanceID].append(body)
                    
                    message.delete()
                    
                messages = status_queue.receive_messages(MaxNumberOfMessages=10)
            
            servers_cleanup_list = []
            
            for server in servers.keys():
                servers[server] = sorted(servers[server], key=lambda message: message["timestamp"])
                
                try:
                    instances = list(ec2.instances.filter(InstanceIds=[server]))
                except:
                    instances = []
                    
                if len(instances) > 0:
                    instance_type = instances[0].instance_type
                    instance_state = instances[0].state["Name"]
                else:
                    instance_type = "unknown"
                    instance_state = "unknown"
                
                if instance_state == "terminated":
                    servers_cleanup_list.append(server)
                
                print ("---------%s %s %s-----------" % (server, instance_type, instance_state))
                last_complete_ts = 0
                last_starting_ts = 0
                last_compleation_time = 0
                
                if printUpdate:
                    for message in servers[server]:
                    
                        ts = datetime.datetime.fromtimestamp(message["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
                        status = message["status"]
                    
                        print(ts, status, message)
                        
                        if status == "starting job":
                            last_starting_ts = float(message["timestamp"])
                        if status == "complete":
                            last_compleation_time = float(message["timestamp"])
                        
                            if last_starting_ts > 0:
                                last_compleation_time = last_compleation_time-last_starting_ts
                    
                    print("--time to compleate last job %f" % (last_compleation_time/60))
                
                
            for server in servers_cleanup_list:
                print("-- server %s is termintated. Time to clean up" % server)
                servers.pop(server)
                
            time.sleep(5)
        

    if action == "add":
    
        if len(blender) <= 0 or len(scene) <= 0:
            print ("ERROR bad values", blender, scene)
            
        else:
            frame_range_list = []
        
            if frame_range[0] == frame_range[1]:
                frame_range_list.append((frame_range[0], frame_range[1]))
            else:
                for i in range (frame_range[0], frame_range[1], chunks):
                    frame_range_list.append((i, min(i+chunks-1, frame_range[1])))
            
            frame_range_list[-1] = (frame_range_list[-1][0], frame_range[1])
            print (frame_range_list)
            
            body = {}
            body["blender"] = blender
            body["scene"] = scene
            body["arch"] = arch

            
            for job in frame_range_list:
            
                body["frame_start"] = job[0]
                body["frame_end"] = job[1]
                body["create_time"] = time.time()
                
                
                MessageGroupId = "%s_%s_%i_%i_%i" % (blender, scene, job[0], job[1], body["create_time"])
                
                response = queue.send_message(MessageBody=json.dumps(body), MessageGroupId=MessageGroupId)

                # The response is NOT a resource, but gives you a message ID and MD5
                print(response.get('MessageId'))
                print(response.get('MD5OfMessageBody'))
            

    if action == "deleteall":
    
        messages = queue.receive_messages(MaxNumberOfMessages=10)
        
        while len(messages) > 0:
        
            for message in messages:
                
                print("deleting ", message.body)
                message.delete()
                
            messages = queue.receive_messages(MaxNumberOfMessages=10)
        
            
