import boto3
import base64
import argparse
import datetime
import time
import logging

now = datetime.datetime.now()

logging.getLogger().setLevel(logging.DEBUG)
logging.basicConfig(filename="batch_trigger.log", level=logging.INFO)


def create_launch_template(
    ec2_client, launch_template_name, key_name, s3fs_mount, s3_bucket
):
    # Use the describe_launch_templates() method to retrieve a list of all launch templates
    launch_template_response = ec2_client.describe_launch_templates()

    # Extract the LaunchTemplates list from the response
    launch_templates = launch_template_response["LaunchTemplates"]

    launch_template_names = [i["LaunchTemplateName"] for i in launch_templates]

    if launch_template_name not in launch_template_names:
        user_data = (
            "MIME-Version: 1.0\nContent-Type: multipart/mixed; boundary="
            "==MYBOUNDARY=="
            "\n\n --==MYBOUNDARY"
            "==\nContent-Type: text/x-shellscript; charset="
            "us-ascii"
            "\n\n#!/bin/bash\n#!/bin/bash -xe\nsudo "
            "amazon-linux-extras install epel -y\nsudo yum install s3fs-fuse -y\nmkdir "
            + s3fs_mount
            + "\nchmod 777 "
            + s3fs_mount
            + "\nsudo s3fs "
            + s3_bucket
            + " "
            + s3fs_mount
            + " -o allow_other -o "
            "umask=000 "
            "-o iam_role=auto"
            "\n\n--==MYBOUNDARY==-- "
        )

        encoded_user_data = base64.b64encode(user_data.encode()).decode()

        response = ec2_client.create_launch_template(
            LaunchTemplateName=launch_template_name,
            VersionDescription="Launch template for running Nextflow on AWS Batch",
            LaunchTemplateData={"KeyName": key_name, "UserData": encoded_user_data},
        )
        logging.info(
            f"{now.strftime('%Y-%m-%d %H:%M:%S')} The Launch Template: {launch_template_name} has been created."
        )
    else:
        logging.info(
            f"{now.strftime('%Y-%m-%d %H:%M:%S')} The Launch Template: {launch_template_name} has already been created."
            f"Hence, moving to the next process"
        )


def create_compute(
    batch_client,
    compute_environment_name,
    allocationStrategy,
    max_vCpus,
    security_groupId,
    subnets,
    instance_role,
    launch_template,
):
    response = batch_client.describe_compute_environments()

    compute_environment_names = [
        env["computeEnvironmentName"] for env in response["computeEnvironments"]
    ]

    if compute_environment_name not in compute_environment_names:
        response1 = batch_client.create_compute_environment(
            computeEnvironmentName=compute_environment_name,
            type="MANAGED",
            state="ENABLED",
            computeResources={
                "type": "EC2",
                "allocationStrategy": allocationStrategy,
                "minvCpus": 0,
                "maxvCpus": max_vCpus,
                # 'imageId': 'string',
                "subnets": subnets,
                "instanceRole": instance_role,
                "securityGroupIds": [
                    security_groupId,
                ],
                "instanceTypes": [
                    "optimal",
                ],
                "launchTemplate": launch_template,
            },
        )
        logging.info(
            f"{now.strftime('%Y-%m-%d %H:%M:%S')} The Compute Environment : {compute_environment_name} has been created."
        )
    else:
        logging.info(
            f"{now.strftime('%Y-%m-%d %H:%M:%S')} The Compute Environment : {compute_environment_name} has already "
            f"been created. "
            f"Hence, moving to the next process"
        )


def create_queue(batch_client, job_queue_name, compute_environment_name):
    response = batch_client.describe_job_queues()
    queue_names = [queue["jobQueueName"] for queue in response["jobQueues"]]
    if job_queue_name not in queue_names:
        response = batch_client.create_job_queue(
            jobQueueName=job_queue_name,
            state="ENABLED",
            priority=1,
            computeEnvironmentOrder=[
                {"order": 100, "computeEnvironment": compute_environment_name},
            ],
        )
        logging.info(
            now.strftime("%Y-%m-%d %H:%M:%S")
            + "The queue : "
            + job_queue_name
            + "has been created."
        )
    else:
        logging.info(
            f"{now.strftime('%Y-%m-%d %H:%M:%S')} The Queue : {job_queue_name} has already been created. "
            f"Hence, moving to the next process"
        )


def create_instance(
    ec2_client,
    key_name,
    instance_role,
    s3_data,
    s3_bucket,
    result_location,
    script_name,
    config_file_name,
    s3_logging_dir,
    s3_result,
    subnet1,
    security_groupId,
):
    instances = ec2_client.run_instances(
        ImageId="ami-0b0dcb5067f052a63",
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
        KeyName=key_name,
        IamInstanceProfile={"Name": instance_role},
        UserData="""#!/bin/bash 
                     sudo rpm --import https://yum.corretto.aws/corretto.key 
                     sudo curl -L -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo 
                     sudo yum install -y  java-11-amazon-corretto-devel 
                     sudo amazon-linux-extras install epel -y
                     sudo yum install s3fs-fuse -y 
                     aws s3 sync """
        + s3_data
        + """ /home/ec2-user/
                     sudo chmod 777 /home/ec2-user/nextflow 
                     cd /home/ec2-user
                     mkdir """
        + result_location
        + """ sudo s3fs """
        + s3_bucket
        + """ """
        + result_location
        + """ -o allow_other -o umask=000 -o iam_role=auto 
                     ./nextflow run /home/ec2-user/"""
        + script_name
        + """ -c """
        + config_file_name
        + """ -bucket-dir """
        + s3_logging_dir
        + """ --outdir="""
        + s3_result
        + """ cd /home/ec2-user/ 
                     touch done.txt
                     aws s3 cp /home/ec2-user/done.txt """
        "s3://" + s3_bucket + "/" + result_location,
        NetworkInterfaces=[
            {
                "AssociatePublicIpAddress": True,
                "DeleteOnTermination": True,
                "DeviceIndex": 0,
                "SubnetId": subnet1,
                "Groups": [security_groupId],
            },
        ],
    )
    logging.info(
        f"{now.strftime('%Y-%m-%d %H:%M:%S')} The instance: {instances['Instances'][0]['InstanceId']} has been created and the nextflow script {script_name} has been triggered"
    )

    return instances["Instances"][0]["InstanceId"]


def check_result(s3, s3_bucket, id_instance, result_location):
    bucket_name = s3_bucket
    file_path = result_location + "done.txt"

    while True:
        try:
            s3.head_object(Bucket=bucket_name, Key=file_path)
        except:
            logging.info(
                f"{now.strftime('%Y-%m-%d %H:%M:%S')} File does not exist in S3 bucket. Checking again in 5 mins."
            )

            time.sleep(300)
        else:
            # File exists, terminate EC2 instance
            instance_id = id_instance
            ec2.terminate_instances(InstanceIds=[instance_id])
            logging.info(
                f"{now.strftime('%Y-%m-%d %H:%M:%S')}. Execution of Script is done and the instance has been terminated."
            )
            break


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--launch_template_name', dest='launch_template_name', required=True)
    parser.add_argument('--key_name', dest='key_name', required=True)
    parser.add_argument('--region_name', dest='region_name', required=True)
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
    parser.add_argument('--subnets', dest='subnets', nargs='+',required=True)
    parser.add_argument('--subnet1', dest='subnet1', required=True)
    parser.add_argument('--result_location', dest='result_location', required=True)
    args = parser.parse_args()

    launch_template_name = args.launch_template_name
    region_name = args.region_name
    key_name = args.key_name
    s3_bucket = args.s3_bucket
    max_vCpus = args.max_vCpus
    s3fs_mount = "/s3fs_mount"
    compute_environment_name = args.compute_environment_name
    allocationStrategy = "BEST_FIT"
    instance_role = args.instance_role
    security_groupId = args.security_groupId
    job_queue_name = args.job_queue_name
    s3_data = args.s3_data
    s3_logging_dir = args.s3_logging_dir
    s3_result = args.s3_result
    script_name = args.script_name
    config_file_name = args.config_file_name
    subnets = args.subnets
    subnet1 = args.subnet1
    result_location = args.result_location
    launch_template = {"launchTemplateName": launch_template_name, "version": "$Latest"}

    ec2_client = boto3.client("ec2", region_name=region_name)
    batch_client = boto3.client("batch", region_name=region_name)
    s3 = boto3.client("s3")

    try:
        create_launch_template(
            ec2_client, launch_template_name, key_name, s3fs_mount, s3_bucket
        )
        create_compute(
            batch_client,
            compute_environment_name,
            allocationStrategy,
            max_vCpus,
            security_groupId,
            subnets,
            instance_role,
            launch_template,
        )
        time.sleep(60)
        create_queue(batch_client, job_queue_name, compute_environment_name)
        instance_id = create_instance(
            ec2_client,
            key_name,
            instance_role,
            s3_data,
            s3_bucket,
            result_location,
            script_name,
            config_file_name,
            s3_logging_dir,
            s3_result,
            subnet1,
            security_groupId,
        )
        check_result(s3, s3_bucket, instance_id, result_location)

    except Exception as e:
        logging.error(
            f"{now.strftime('%Y-%m-%d %H:%M:%S')} Following error occurred: {str(e)}"
        )


if __name__ == "__main__":
    main()
