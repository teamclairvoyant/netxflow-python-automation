import boto3
import base64
import argparse
import datetime
import time
import logging

logging.getLogger().setLevel(logging.DEBUG)
logging.basicConfig(filename="batch_pipeline.log", level=logging.INFO)


def determine_maxvcpus(no_of_instances, instances, instance_type):
    if instance_type not in instances.keys():
        raise Exception("Not a valid instance type")
    else:
        if no_of_instances <= 100:
            cores = no_of_instances * instances[instance_type]
            return cores
        else:
            logging.error("Maximum instance limit reached")
            raise Exception("Maximum instance limit reached")


def create_launch_template(
        ec2_client,
        launch_template_name,
        key_name,
        region_name,
        s3fs_mount,
        s3_bucket,
        secret_id,
        output_location
):
    # Use the describe_launch_templates() method to retrieve a list of all launch templates
    launch_template_response = ec2_client.describe_launch_templates()

    # Extract the LaunchTemplates list from the response
    launch_templates = launch_template_response["LaunchTemplates"]

    launch_template_names = [i["LaunchTemplateName"] for i in launch_templates]

    if launch_template_name not in launch_template_names:
        user_data = f"""MIME-Version: 1.0\nContent-Type: multipart/mixed; boundary="==MYBOUNDARY=="\n\n--==MYBOUNDARY==
Content-Type: text/cloud-config; charset="us-ascii"\n\npackages:\n- jq\n- aws-cli\n
runcmd:
- amazon-linux-extras install epel -y
- sudo sed -i 's/enabled=0/enabled=1/' /etc/yum.repos.d/epel.repo
- sudo yum install -y gcc libstdc++-devel gcc-c++ fuse fuse-devel curl-devel libxml2-devel mailcap automake openssl-devel git wget
- wget https://github.com/s3fs-fuse/s3fs-fuse/archive/refs/tags/v1.91.tar.gz
- tar -xvzf v1.91.tar.gz
- cd s3fs-fuse-1.91/
- ./autogen.sh
- ./configure --prefix=/usr --with-openssl
- make
- sudo make install
- cd /
- /usr/bin/aws configure set region {region_name}
- export SECRET_STRING=$(/usr/bin/aws secretsmanager get-secret-value --secret-id {secret_id} | jq -r '.SecretString')
- export USERNAME=$(echo $SECRET_STRING | jq -r '.username')
- export PASSWORD=$(echo $SECRET_STRING | jq -r '.password')
- export REGISTRY_URL=$(echo $SECRET_STRING | jq -r '.registry_url')
- echo $PASSWORD | docker login --username $USERNAME --password-stdin $REGISTRY_URL
- export AUTH=$(cat ~/.docker/config.json | jq -c .auths)
- echo 'ECS_ENGINE_AUTH_TYPE=dockercfg' >> /etc/ecs/ecs.config
- echo "ECS_ENGINE_AUTH_DATA=$AUTH" >> /etc/ecs/ecs.config
- mkdir {s3fs_mount} 
- cd /home/ec2-user
- mkdir rnaseq
- chmod 777 {s3fs_mount} 
- sudo s3fs {s3_bucket} {s3fs_mount} -o allow_other -o umask=000 -o iam_role=auto
- sudo s3fs {s3_bucket}:{output_location} /home/ec2-user/rnaseq -o allow_other -o umask=000 -o iam_role=auto\n
--==MYBOUNDARY==--\n"""

        encoded_user_data = base64.b64encode(user_data.encode()).decode()

        response = ec2_client.create_launch_template(
            LaunchTemplateName=launch_template_name,
            VersionDescription="Launch template for running Nextflow on AWS Batch",
            LaunchTemplateData={"KeyName": key_name, "UserData": encoded_user_data},
        )
        logging.info(f"The Launch Template: {launch_template_name} has been created.")
    else:
        logging.info(
            f"The Launch Template: {launch_template_name} has already been created."
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
        instance_type
):
    response = batch_client.describe_compute_environments()

    compute_environment_names = [
        env["computeEnvironmentName"] for env in response["computeEnvironments"]
    ]

    if compute_environment_name not in compute_environment_names:
        response = batch_client.create_compute_environment(
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
                "instanceTypes":
                    instance_type,
                "launchTemplate": launch_template,
            },
        )
        logging.info(
            f"The Compute Environment : {compute_environment_name} has been created."
        )
    else:
        logging.info(
            f"The Compute Environment : {compute_environment_name} has already "
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
        logging.info(f"The queue: {job_queue_name} has been created.")
    else:
        logging.info(
            f"The Queue : {job_queue_name} has already been created. "
            f"Hence, moving to the next process"
        )


def create_instance(
        ec2_client,
        key_name,
        instance_role,
        s3_data,
        s3_bucket,
        output_location,
        result_location,
        availability_zone,
        script_name,
        config_file_name,
        s3_logging_dir,
        endpoint,
        analysesId,
        projectId,
        s3_result,
        subnet1,
        security_groupId,
        success_status,
        failure_status
):
    instances = ec2_client.run_instances(
        ImageId="ami-02d5619017b3e5162",
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
        KeyName=key_name,
        IamInstanceProfile={"Name": instance_role},
        UserData=f"""#!/bin/bash 
                 sudo rpm --import https://yum.corretto.aws/corretto.key 
                 sudo curl -L -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo 
                 sudo yum install -y  java-11-amazon-corretto-devel 
                 sudo amazon-linux-extras install epel -y
                 sudo yum install s3fs-fuse -y 
                 aws s3 sync {s3_data} /home/ec2-user/
                 sudo chmod 777 /home/ec2-user/nextflow 
                 cd /home/ec2-user
                 mkdir {result_location}
                 sudo s3fs {s3_bucket}:{output_location} {result_location} -o allow_other -o umask=000 -o iam_role=auto 
                 ./nextflow run /home/ec2-user/{script_name} -c {config_file_name} -bucket-dir {s3_logging_dir} --outdir={s3_result} > main_log.log
                 cd /home/ec2-user/ 
                 aws s3 cp /home/ec2-user/main_log.log {s3_logging_dir}
                 touch done.txt
                 if grep -q "Succeeded" "/home/ec2-user/main_log.log"; then
                     curl -X 'PATCH' {endpoint} -H 'accept: */*' -H 'Content-Type: application/json' -d '{{ "analysesId": {analysesId}, "projectId": {projectId}, "status": {success_status}}}'
                 else
                     curl -X 'PATCH' {endpoint} -H 'accept: */*' -H 'Content-Type: application/json' -d '{{ "analysesId": {analysesId}, "projectId": {projectId}, "status": {failure_status}}}' 
                 fi
                 aws s3 cp /home/ec2-user/done.txt s3://{s3_bucket}{output_location}
                 instance_id=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
                 aws ec2 terminate-instances --instance-ids $instance_id""",
        Placement={
            'AvailabilityZone': availability_zone
        },
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
        f"The instance: {instances['Instances'][0]['InstanceId']} has been created and the nextflow script {script_name} has been triggered "
    )

    return instances["Instances"][0]["InstanceId"]


def terminate(ec2_client, batch_client, s3, s3_bucket, id_instance, output_location, timeout, job_queue_name,
              compute_environment_name, launch_template_name):
    bucket_name = s3_bucket
    file_path = output_location[1:] + "done.txt"
    start_time = time.time()

    while True:
        elapsed_time = time.time() - start_time
        if elapsed_time > timeout:
            print("Timout occured so terminating the process")
            instance_id = id_instance
            ec2_client.terminate_instances(InstanceIds=[instance_id])
            logging.warning(
                "Execution of Script is taking longer than expected and hence the instance has been terminated."
            )
            break

        try:
            s3.head_object(Bucket=bucket_name, Key=file_path)

        except:
            logging.info(" Process is not yet successful. Checking again in 5 mins.")

            time.sleep(300)
        else:
            instance_id = id_instance
            ec2_client.terminate_instances(InstanceIds=[instance_id])
            logging.info(
                " Execution of Script is done and the instance has been terminated."
            )

            response = batch_client.update_job_queue(
                jobQueue=job_queue_name,
                state='DISABLED',
            )

            time.sleep(60)
            response = batch_client.delete_job_queue(jobQueue=job_queue_name)
            time.sleep(60)

            response = batch_client.update_compute_environment(computeEnvironment=compute_environment_name,
                                                               state='DISABLED',
                                                               )
            time.sleep(60)
            response = batch_client.delete_compute_environment(computeEnvironment=compute_environment_name)

            response = ec2_client.delete_launch_template(
                LaunchTemplateName=launch_template_name
            )


def main():
    instances = {"m5.large": 2, "m5.xlarge": 4, "m5.2xlarge": 8, "m5.4xlarge": 16, "m5.8xlarge": 32, "m5.12xlarge": 48,
                 "m5.16xlarge": 64, "m5.24xlarge": 96, "m5.metal": 96, "m5d.large": 2, "m5d.xlarge": 4,
                 "m5d.2xlarge": 8,
                 "m5d.4xlarge": 16, "m5d.8xlarge": 32, "m5d.12xlarge": 48, "m5d.16xlarge": 64, "m5d.24xlarge": 96,
                 "m5d.metal": 96
                 }
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--launch_template_name", dest="launch_template_name", required=True
    )
    parser.add_argument("--key_name", dest="key_name", required=True)
    parser.add_argument("--region_name", dest="region_name", required=True)
    parser.add_argument("--availability_zone", dest='availability_zone', required=True)
    parser.add_argument("--s3_bucket", dest="s3_bucket", required=True)
    parser.add_argument("--s3_logging_dir", dest="s3_logging_dir", required=True)
    parser.add_argument("--s3_result", dest="s3_result", required=True)
    parser.add_argument('--no_of_instances', dest='no_of_instances', type=int, required=True)
    parser.add_argument('--instance_type', dest='instance_type', required=True)
    parser.add_argument(
        "--compute_environment_name", dest="compute_environment_name", required=True
    )
    parser.add_argument("--instance_role", dest="instance_role", required=True)
    parser.add_argument("--security_groupId", dest="security_groupId", required=True)
    parser.add_argument("--job_queue_name", dest="job_queue_name", required=True)
    parser.add_argument("--s3_data", dest="s3_data", required=True)
    parser.add_argument("--script_name", dest="script_name", required=True)
    parser.add_argument("--config_file_name", dest="config_file_name", required=True)
    parser.add_argument("--subnets", dest="subnets", nargs="+", required=True)
    parser.add_argument("--result_location", dest="result_location", required=True)
    parser.add_argument("--output_location", dest="output_location", required=True)
    parser.add_argument("--secret_id", dest="secret_id", required=True)
    parser.add_argument("--analysesId", dest="analysesId", required=True)
    parser.add_argument("--projectId", dest="projectId", required=True)
    parser.add_argument("--endpoint", dest="endpoint", required=True)
    parser.add_argument("--success_status", dest="success_status", required=True)
    parser.add_argument("--failure_status", dest="failure_status", required=True)

    args = parser.parse_args()

    launch_template_name = args.launch_template_name
    region_name = args.region_name
    availability_zone = args.availability_zone
    key_name = args.key_name
    s3_bucket = args.s3_bucket
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
    subnet1 = subnets[0]
    timeout = 25200
    result_location = args.result_location
    output_location = args.output_location
    secret_id = args.secret_id
    launch_template = {"launchTemplateName": launch_template_name, "version": "$Latest"}
    instance_type = args.instance_type
    instance = [args.instance_type]
    endpoint = args.endpoint
    analysesId = args.analysesId
    projectId = args.projectId
    success_status = args.success_status
    failure_status = args.failure_status

    no_of_instances = args.no_of_instances

    ec2_client = boto3.client("ec2", region_name=region_name)
    batch_client = boto3.client("batch", region_name=region_name)
    s3 = boto3.client("s3")

    try:
        max_vCpus = determine_maxvcpus(no_of_instances, instances, instance_type)
        create_launch_template(
            ec2_client, launch_template_name, key_name, region_name, s3fs_mount, s3_bucket, secret_id, output_location
        )
        time.sleep(60)
        create_compute(
            batch_client,
            compute_environment_name,
            allocationStrategy,
            max_vCpus,
            security_groupId,
            subnets,
            instance_role,
            launch_template,
            instance
        )
        time.sleep(60)
        create_queue(batch_client, job_queue_name, compute_environment_name)
        instance_id = create_instance(
            ec2_client,
            key_name,
            instance_role,
            s3_data,
            s3_bucket,
            output_location,
            result_location,
            availability_zone,
            script_name,
            config_file_name,
            s3_logging_dir,
            endpoint,
            analysesId,
            projectId,
            s3_result,
            subnet1,
            security_groupId,
            success_status,
            failure_status
        )
        terminate(ec2_client, batch_client, s3, s3_bucket, instance_id, output_location, timeout, job_queue_name,
                  compute_environment_name, launch_template)

    except Exception as e:
        logging.error(f"Following error occurred: {str(e)}")


if __name__ == "__main__":
    main()
