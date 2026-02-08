import os
from dotenv import load_dotenv

load_dotenv()

accounts_map = {
    "digiwatt": os.environ["AWS_ACCOUNT_ID__DIGIWATT"],
    "sinapsi_prod": os.environ["AWS_ACCOUNT_ID__SINAPSI_PROD"],
    "fastweb_prod": os.environ["AWS_ACCOUNT_ID__FASTWEB_PROD"],
    "fastweb_staging": os.environ["AWS_ACCOUNT_ID__FASTWEB_STAGING"],
    "fastweb_dev": os.environ["AWS_ACCOUNT_ID__FASTWEB_DEV"],
}

roles_arn_map = {
    name: {
        "costs": f"arn:aws:iam::{account_id}:role/{os.environ['AWS_ROLE__COSTS']}",
        "infra": f"arn:aws:iam::{account_id}:role/{os.environ['AWS_ROLE__INFRA']}",
    }
    for name, account_id in accounts_map.items()
}

service_map = {
    "AWS Cloud Map": "Cloud Map",
    "AWS CloudShell": "CloudShell",
    "AWS CloudTrail": "CloudTrail",
    "AWS Config": "Config",
    "AWS Glue": "Glue",
    "AWS IoT": "IoT",
    "AWS IoT Device Management": "IoT Device Management",
    "AWS Key Management Service": "KMS",
    "AWS Lambda": "Lambda",
    "AWS Payment Cryptography": "Payment Cryptography",
    "AWS Secrets Manager": "Secrets Manager",
    "AWS Security Hub": "Security Hub",
    "AWS Step Functions": "Step Functions",
    "AWS Support (Developer)": "Support (Developer)",
    "AWS Systems Manager": "Systems Manager",
    "AWS WAF": "WAF",
    "AWS X-Ray": "X-Ray",
    "Amazon API Gateway": "API Gateway",
    "Amazon Athena": "Athena",
    "Amazon CloudFront": "CloudFront",
    "Amazon DocumentDB (with MongoDB compatibility)": "DocumentDB",
    "Amazon DynamoDB": "DynamoDB",
    "Amazon EC2 Container Registry (ECR)": "ECR",
    "Amazon Elastic Compute Cloud - Compute": "Elastic Compute Cloud",
    "Amazon Elastic Container Service": "ECS",
    "Amazon Elastic File System": "EFS",
    "Amazon Elastic Load Balancing": "ELB",
    "Amazon GuardDuty": "GuardDuty",
    "Amazon Inspector": "Inspector",
    "Amazon Kinesis": "Kinesis",
    "Amazon Kinesis Firehose": "Kinesis Firehose",
    "Amazon Location Service": "Location Service",
    "Amazon Macie": "Macie",
    "Amazon Relational Database Service": "RDS",
    "Amazon Route 53": "Route 53",
    "Amazon Simple Notification Service": "SNS",
    "Amazon Simple Storage Service": "Simple Storage Service",
    "Amazon Timestream": "Timestream",
    "Amazon Virtual Private Cloud": "VPC",
    "AmazonCloudWatch": "CloudWatch",
    "CloudWatch Events": "CloudWatch Events",
    "EC2 - Other": "EC2 - Other",
    "Tax": "Tax",
}
