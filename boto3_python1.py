import boto3


def create_instance_1():
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    instances = ec2_client.run_instances(
        BlockDeviceMappings=[
            {
                'DeviceName': '/dev/xvda',
                'Ebs': {

                    'DeleteOnTermination': True,
                    'VolumeSize': 100,
                    'VolumeType': 'gp2'
                },
            },
        ],
        ImageId="ami-0b0dcb5067f052a63",
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
        KeyName="harshal_nextflow_ec2_test",
        IamInstanceProfile={'Name': 'Test_hd'},
        UserData="""#!/bin/bash 
        aws s3 sync s3://harshal-redshift-training/nextflow/APPS/ /home/ec2-user/"""
    )

    instance_id = instances["Instances"][0]["InstanceId"]
    return instance_id


instance_id_1 = create_instance_1()


def create_ami():
    ec2 = boto3.client("ec2")
    image = ec2.create_image(InstanceId=instance_id_1, NoReboot=True, Name="test123")
    ami_id = image.id
    return ami_id


def create_compute():
    client = boto3.client('batch')

    response1 = client.create_compute_environment(
        computeEnvironmentName='new_environment',
        type='MANAGED',
        state='ENABLED',
        computeResources={
            'type': 'EC2',
            'allocationStrategy': 'BEST_FIT',
            'minvCpus': 0,
            'maxvCpus': 256,
            # 'imageId': 'string',
            'subnets': [
                'subnet-fac7fca3',
                'subnet-d49e41e9',
                'subnet-0831746d',
                'subnet-c3557ee8',
                'subnet-935abbe5',
                'subnet-e89604e4'
            ],
            'instanceRole': 'Test_hd',
            'securityGroupIds': [
                'sg-97b165f1',
            ],
            'instanceTypes': [
                'optimal',
            ]
        }
    )

    print(response1)


create_compute()


def create_queue():
    client = boto3.client('batch')
    response = client.create_job_queue(
        jobQueueName='new_import_queue',
        state='ENABLED',
        priority=1,
        computeEnvironmentOrder=[
            {
                'order': 100,
                'computeEnvironment': 'new_environment'
            },
        ],
    )

    print(response)


create_queue()


def create_instance():
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    instances = ec2_client.run_instances(
        ImageId="ami-0b0dcb5067f052a63",
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
        KeyName="harshal_nextflow_ec2_test",
        IamInstanceProfile={'Name': 'Test_hd'},
        UserData="""#!/bin/bash 
        sudo rpm --import https://yum.corretto.aws/corretto.key 
        sudo curl -L -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo 
        sudo yum install -y  java-11-amazon-corretto-devel 
        aws s3 sync s3://harshal-redshift-training/nextflow/nextflow_batch/ /home/ec2-user/
        sudo chmod 777 /home/ec2-user/nextflow 
        cd /home/ec2-user 
        ./nextflow run /home/ec2-user/RNASeq.nf -c nextflow.config_updated_new -bucket-dir s3://harshal-redshift-training/temp --outdir=s3://harshal-redshift-training/batch"""
    )

    print(instances["Instances"][0]["InstanceId"])


create_instance()


# def check_job():
#     client = boto3.client('batch')
#     response = client.list_jobs(
#         jobQueue='string',
#         arrayJobId='string',
#         multiNodeJobId='string',
#         jobStatus= 'SUCCEEDED' | 'FAILED',
#         maxResults=123,
#         nextToken='string',
#         filters=[
#             {
#                 'name': 'string',
#                 'values': [
#                     'string',
#                 ]
#             },
#         ]
#     )
