import boto3
import base64
import argparse
import datetime
import time

ec2 = boto3.client('ec2', region_name="us-east-1")
ec2_client = boto3.client("batch", region_name="us-east-1")
now = datetime.datetime.now()
timestamp_str = "[" + now.strftime("%Y-%m-%d %H:%M:%S") + "]"

parser = argparse.ArgumentParser()
parser.add_argument('--launch_template_name', dest='launch_template_name', required=True)
parser.add_argument('--key_name', dest='key_name', required=True)
parser.add_argument('--s3_bucket', dest='s3_bucket', required=True)
parser.add_argument('--s3_logging_dir', dest='s3_logging_dir', required=True)
parser.add_argument('--s3_result', dest='s3_result', required=True)
parser.add_argument('--max_vCpus', dest='max_vCpus', required=True)
parser.add_argument('--compute_environment_name', dest='compute_environment_name', required=True)
parser.add_argument('--instance_role', dest='instance_role', required=True)
parser.add_argument('--security_groupId', dest='security_groupId', required=True)
parser.add_argument('--job_queue_name', dest='job_queue_name', required=True)
parser.add_argument('--s3_data', dest='s3_data', required=True)
parser.add_argument('--script_name', dest='script_name', required=True)
parser.add_argument('--config_file_name', dest='config_file_name', required=True)
parser.add_argument('--subnet1', dest='subnet1', required=True)
parser.add_argument('--subnet2', dest='subnet2', required=True)
parser.add_argument('--subnet3', dest='subnet3', required=True)
parser.add_argument('--subnet4', dest='subnet4', required=True)
parser.add_argument('--subnet5', dest='subnet5', required=True)
parser.add_argument('--subnet6', dest='subnet6', required=True)
parser.add_argument('--result_location', dest='result_location', required=True)
args = parser.parse_args()

launch_template_name = args.launch_template_name
key_name = args.key_name
s3_bucket = args.s3_bucket
max_vCpus = args.max_vCpus
s3fs_mount = '/test'
compute_environment_name = args.compute_environment_name
allocationStrategy = 'optimal'
instance_role = args.instance_role
security_groupId = args.security_groupId
job_queue_name = args.job_queue_name
s3_data = args.s3_data
s3_logging_dir = args.s3_logging_dir
script_name = args.script_name
config_file_name = args.config_file_name
subnet1 = args.subnet1
subnet2 = args.subnet2
subnet3 = args.subnet3
subnet4 = args.subnet4
subnet5 = args.subnet5
subnet6 = args.subnet6
result_location = args.result_location


def create_launch_template():
    # Use the describe_launch_templates() method to retrieve a list of all launch templates
    launch_template_response = ec2.describe_launch_templates()

    # Extract the LaunchTemplates list from the response
    launch_templates = launch_template_response['LaunchTemplates']

    launch_template_names = [i['LaunchTemplateName'] for i in launch_templates]

    if launch_template_name not in launch_template_names:
        user_data = "MIME-Version: 1.0\nContent-Type: multipart/mixed; boundary=""==MYBOUNDARY==""\n\n --==MYBOUNDARY" \
                    "==\nContent-Type: text/x-shellscript; charset=""us-ascii""\n\n#!/bin/bash\n#!/bin/bash -xe\nsudo " \
                    "amazon-linux-extras install epel -y\nsudo yum install s3fs-fuse -y\nmkdir " \
                    "/test\nchmod 777 /test\nsudo s3fs " \
                    + s3_bucket + "/test -o allow_other -o " \
                                  "umask=000 " \
                                  "-o iam_role=auto""\n\n--==MYBOUNDARY==-- "

        encoded_user_data = base64.b64encode(user_data.encode()).decode()

        response = ec2.create_launch_template(
            LaunchTemplateName=launch_template_name,
            VersionDescription='this is test',
            LaunchTemplateData={
                'KeyName': key_name,
                'UserData': encoded_user_data
            }
        )
    else:
        print(
            timestamp_str + "The Launch Template : " + launch_template_name + "has already been created. Hence, moving with next "
                                                                              "process")


def create_compute():
    response = ec2_client.describe_compute_environments()

    compute_environment_names = [env['computeEnvironmentName'] for env in response['computeEnvironments']]

    if computeEnvironmentName not in compute_environment_names:
        response1 = client.create_compute_environment(
            computeEnvironmentName=compute_environment_name,
            type='MANAGED',
            state='ENABLED',
            computeResources={
                'type': 'EC2',
                'allocationStrategy': allocationStrategy,
                'minvCpus': 0,
                'maxvCpus': max_vCpus,
                # 'imageId': 'string',
                'subnets': [
                    subnet1,
                    subnet2,
                    subnet3,
                    subnet4,
                    subnet5,
                    subnet6
                ],
                'instanceRole': instance_role,
                'securityGroupIds': [
                    security_groupId,
                ],
                'instanceTypes': [
                    'optimal',
                ]
            }
        )
        print(response1)
    else:
        print(timestamp_str +
              "The Compute Environment : " + compute_environment_name + "has already been created. Hence, moving with the next process")


def create_queue():
    response = ec2_client.describe_job_queues()
    queue_names = [queue['jobQueueName'] for queue in response['jobQueues']]
    if jobQueueName not in queue_names:
        response = client.create_job_queue(
            jobQueueName=job_queue_name,
            state='ENABLED',
            priority=1,
            computeEnvironmentOrder=[
                {
                    'order': 100,
                    'computeEnvironment': compute_environment_name
                },
            ],
        )
        print(response)
    else:
        print(timestamp_str +
              "The Compute Environment : " + compute_environment_name + "has already been created. Hence, moving with the next process")


def create_instance():
    instances = ec2.run_instances(
        ImageId="ami-0b0dcb5067f052a63",
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
        KeyName=key_name,
        IamInstanceProfile={'Name': instance_role},
        UserData="""#!/bin/bash 
                     sudo rpm --import https://yum.corretto.aws/corretto.key 
                     sudo curl -L -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo 
                     sudo yum install -y  java-11-amazon-corretto-devel 
                     sudo amazon-linux-extras install epel -y
                     sudo yum install s3fs-fuse -y 
                     aws s3 sync """ + s3_data + """ /home/ec2-user/
                     sudo chmod 777 /home/ec2-user/nextflow 
                     cd /home/ec2-user
                     mkdir rnaseq
                     sudo s3fs """ + s3_bucket + """ """ + result_location + """ -o allow_other -o umask=000 -o iam_role=auto 
                     ./nextflow run /home/ec2-user/""" + script_name + """-c """ + config_file_name + """ -bucket-dir """ + s3_logging_dir + """ --outdir=""" + s3_result
                     + """ cd /home/ec2-user/ 
                     touch done.txt
                     aws s3 cp /home/ec2-user/done.txt s3://harshal-redshift-training/2000864/RNA-Seq_Pipeline1.0/""",
        NetworkInterfaces=[
            {
                'AssociatePublicIpAddress': True,
                'DeleteOnTermination': True,
                'DeviceIndex': 0,
                'SubnetId': subnet1,
                'Groups': [security_groupId],
            },
        ]
    )
    print(timestamp_str +
          "The instance : " + instances["Instances"][0][
              "InstanceId"] + "has been created and the nextflow script" + script_name + "has been triggered")
    return instances["Instances"][0]["InstanceId"]


def check_result(id1):
    bucket_name = s3_bucket
    file_path = result_location + "done.txt"
    s3 = boto3.client('s3')

    while True:
        try:
            s3.head_object(Bucket=bucket_name, Key=file_path)
        except:
            print(timestamp_str + "File does not exist in S3 bucket. Checking again in 5 mins.")
            time.sleep(300)
        else:
            # File exists, terminate EC2 instance
            instance_id = id1
            ec2.terminate_instances(InstanceIds=[instance_id])
            print("EC2 instance terminated.")
            break


def main():
    try:
        create_launch_template()
        create_compute()
        create_queue()
        instance_id = create_instance()
        create_result(instance_id)
    except Exception as e:
        print("Following error occurred  : " + str(e))


if __name__ == "__main__":
    main()
