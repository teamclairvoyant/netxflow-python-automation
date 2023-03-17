# nextflow-python-automation

### To run Nextflow on AWS, we need to integrate it with AWS Batch which act as an executor for Nextflow on AWS. However, there are some settings which needs to be done in AWS batch such as creation and configuration of Job Queue and Compute environment.
Here the above code does all these configurations based on the below parameters which should be passed to the code as arguments. 


# List of parameters :

1.	Launch Template Name - A launch template contains the parameters to launch an instance. This launch template will be used by AWS Batch to trigger executor EC2 instances.    


2.	Key name – Any instance triggered using the code or AWS can be accessed using this provided key pair.

3.	Region Name – AWS region to launch ec2 instances.

4.	S3_bucket – The master S3 bucket.

5.	S3_logging_dir – The S3 location where nextflow should store logs.

6.	S3_result – The S3 location where Nextflow divert the output reports. *

7.	max_vCpus – The maximum number of vCPUs for compute environment.

8.	compute_envrionment_name – The name for compute environment which the code will create. 

9.	instance_role – The instance role which should be attached ec2 instances triggered by both code and AWS batch.

10.	security_groupId – The security group which should be attached ec2 instances triggered by both code and AWS batch.

11.	job_queue_name – The name for job queue.

12.	s3_data – The exact s3 location where data required for Nextflow is situated.

13.	script_name – The Nextflow script name.


14.	config_file_name – The configuration file name to trigger Nextflow script.

15.	subnet1 – Subnet to be given to compute environment to launch EC2 instances.

16.	subnet2 – Subnet to be given to compute environment to launch EC2 instances.

17.	subnet3 - Subnet to be given to compute environment to launch EC2 instances.

18.	subnet4 – Subnet to be given to compute environment to launch EC2 instances.

19.	subnet5 – Subnet to be given to compute environment to launch EC2 instances.

20.	subnet6- Subnet to be given to compute environment to launch EC2 instances.

21.	result_location – The S3 location where Nextflow divert the output reports.

22.	s3fs_mount – The name for s3fs mount on ec2 instance. 

23.	allocationStrategy – The allocationStrategy to launch ec2 instance (or the name of EC2 instance)
