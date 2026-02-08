import os
import boto3
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timezone
from utils import roles_arn_map

load_dotenv()
REGION = "eu-central-1"


class AwsCostsClient:
    def __init__(self, account):
        self.account = account
        self.expiration = 0
        role_arn = roles_arn_map[account]["costs"]
        self.client = boto3.client("ce", **self.assume_role(role_arn))

    def role_is_expired(self):
        return datetime.now(timezone.utc) >= self.expiration

    def assume_role(self, role_arn, session_name="CollectorSession"):
        sts_client = boto3.client("sts")
        resp = sts_client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
        creds = resp["Credentials"]
        self.expiration = creds["Expiration"]
        return {
            "aws_access_key_id": creds["AccessKeyId"],
            "aws_secret_access_key": creds["SecretAccessKey"],
            "aws_session_token": creds["SessionToken"],
            "region_name": REGION,
        }

    def refresh_connection(self):
        if self.role_is_expired():
            role_arn = roles_arn_map[self.account]["costs"]
            self.client = boto3.client("ce", **self.assume_role(role_arn))

    def get_records(self, start, stop, format="dict"):
        self.refresh_connection()
        token = None
        results = []

        while True:
            kwargs = dict(
                TimePeriod={"Start": start.isoformat(), "End": stop.isoformat()},
                Granularity="DAILY",
                Metrics=["UnblendedCost"],
                GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
            )
            if self.account == "digiwatt":
                kwargs["Filter"] = {
                    "Not": {"Tags": {"Key": "Publisher", "Values": ["terraform"]}}
                }
            if token:
                kwargs["NextPageToken"] = token

            resp = self.client.get_cost_and_usage(**kwargs)
            results.extend(resp["ResultsByTime"])

            token = resp.get("NextPageToken")
            if not token:
                break
        records = []
        total = 0
        for day in results:
            date = day["TimePeriod"]["Start"]
            for group in day["Groups"]:
                service = group["Keys"][0]
                amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
                if amount > 0:
                    total += amount
                    match format:
                        case "dict":
                            records.append(
                                {
                                    "date": date,
                                    "account": self.account,
                                    "service": service,
                                    "amount": amount,
                                }
                            )
                        case "tuple":
                            # print((date, ACCOUNT, service, amount))
                            records.append((date, self.account, service, amount))

        print(total)
        return records


def get_aws_costs_client(account):
    return AwsCostsClient(account)
