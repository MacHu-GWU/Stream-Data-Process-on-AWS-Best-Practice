# -*- coding: utf-8 -*-

import json

from troposphere_mate import (
    Template, Parameter, iam, kinesis, firehose, helper_fn_sub, Ref, Sub,
    AWS_ACCOUNT_ID,
    glue,
    upload_template, deploy_stack,

)
from troposphere_mate.canned.iam import (
    create_assume_role_policy_document,
    AWSServiceName,
    AWSManagedPolicyArn,
)

template = Template()

param_env_name = Parameter(
    "EnvironmentName",
    Type="String",
)

# Input Stream
kinesis_input_stream = kinesis.Stream(
    "KinesisInputStream",
    template=template,
    Name=helper_fn_sub("{}-web-event", param_env_name),
    RetentionPeriodHours=24,
    ShardCount=1,
)

# Output Delivery Stream
kinesis_delivery_stream_role = iam.Role(
    "KinesisDeliveryStreamServiceRole",
    template=template,
    RoleName=helper_fn_sub("{}-kinesis-delivery-stream-service-role", param_env_name),
    AssumeRolePolicyDocument=create_assume_role_policy_document(
        [
            AWSServiceName.amazon_Kinesis_Data_Firehose,
        ]
    ),
    ManagedPolicyArns=[
        AWSManagedPolicyArn.administratorAccess,
    ]
)

kinesis_delivery_stream = firehose.DeliveryStream(
    "KinesisDeliveryStream",
    template=template,
    DeliveryStreamName=helper_fn_sub("{}-web-event", param_env_name),
    DeliveryStreamType="DirectPut",
    ExtendedS3DestinationConfiguration=firehose.ExtendedS3DestinationConfiguration(
        BucketARN="arn:aws:s3:::eq-sanhe-for-everything",
        Prefix=Sub("data/kinesis-analytics/${EnvironmentName}/year=!{timestamp:YYYY}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/minute=!{timestamp:mm}/", {"EnvironmentName": Ref(param_env_name)}),
        ErrorOutputPrefix=Sub("data/kinesis-analytics/${EnvironmentName}/result=!{firehose:error-output-type}/year=!{timestamp:YYYY}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/minute=!{timestamp:mm}/", {"EnvironmentName": Ref(param_env_name)}),
        BufferingHints=firehose.BufferingHints(
            IntervalInSeconds=60,
            SizeInMBs=5
        ),
        CompressionFormat="UNCOMPRESSED",
        RoleARN=kinesis_delivery_stream_role.iam_role_arn,
        S3BackupMode="Disabled",
    )
)

# Kinesis Analytics Application
kinesis_analytics_application_role = iam.Role(
    "KinesisAnalyticsApplicationServiceRole",
    template=template,
    RoleName=helper_fn_sub("{}-kinesis-analytics-service-role", param_env_name),
    AssumeRolePolicyDocument=create_assume_role_policy_document(
        [
            AWSServiceName.amazon_Kinesis_Data_Analytics,
        ]
    ),
    ManagedPolicyArns=[
        AWSManagedPolicyArn.administratorAccess,
    ]
)

INPUT_STREAM_NAME_PREFIX = "SOURCE_SQL_STREAM"
OUTPUT_STREAM_NAME = "DESTINATION_SQL_STREAM"
SQL = """
CREATE OR REPLACE STREAM \"{OUTPUT_STREAM_NAME}\" (\"event_time\" TIMESTAMP, \"sign_up_event_counts\" INTEGER);
CREATE OR REPLACE PUMP \"STREAM_PUMP\" AS INSERT INTO \"{OUTPUT_STREAM_NAME}\"
SELECT STREAM
    FLOOR((MONOTONIC(\"{INPUT_NAME_PREFIX}_001\".\"event_time\") - TIMESTAMP '1970-01-01 00:00:00') SECOND / 60 TO SECOND) * 60 + TIMESTAMP '1970-01-01 00:00:00' as \"event_time\",
    COUNT(*) as \"sign_up_event_counts\"
FROM \"{INPUT_NAME_PREFIX}_001\"
WHERE \"event_name\" SIMILAR TO '%sign_up%'\n
GROUP BY 
    FLOOR((MONOTONIC(\"{INPUT_NAME_PREFIX}_001\".\"event_time\") - TIMESTAMP '1970-01-01 00:00:00') SECOND / 60 TO SECOND) * 60 + TIMESTAMP '1970-01-01 00:00:00';
""".format(
    INPUT_NAME_PREFIX=INPUT_STREAM_NAME_PREFIX,
    OUTPUT_STREAM_NAME=OUTPUT_STREAM_NAME
)

# NOTE: the kinesis analytics console can only support V1 API, if you create
# it with V2 api, the console cannot display anything
# BUT, troposphere doesn't comes with v1 api, so you can only inject raw json
# into the template content.
kinesis_analytics_application_logic_id = "KinesisAnalyticsApplication"
kinesis_analytics_application_name = {
    "Fn::Sub": [
        "${%s}-sign-up-metrics" % param_env_name.title,
        {param_env_name.title: {"Ref": param_env_name.title}}
    ]
}
kinesis_analytics_application_data = {
    "Type": "AWS::KinesisAnalytics::Application",
    "Properties": {
        "ApplicationName": kinesis_analytics_application_name,
        "Inputs": [
            {
                "InputParallelism": {"Count": 1},
                "KinesisStreamsInput": {
                    "ResourceARN": {"Fn::GetAtt": [kinesis_input_stream.title, "Arn"], },
                    "RoleARN": {"Fn::GetAtt": [kinesis_analytics_application_role.title, "Arn"]},
                },
                "NamePrefix": INPUT_STREAM_NAME_PREFIX,
                "InputSchema": {
                    "RecordFormat": {
                        "RecordFormatType": "JSON",
                        "MappingParameters": {
                            "JSONMappingParameters": {
                                "RecordRowPath": "$"
                            }
                        }
                    },
                    "RecordEncoding": "UTF-8",
                    "RecordColumns": [
                        {
                            "Name": "event_id",
                            "Mapping": "$.event_id",
                            "SqlType": "VARCHAR(64)"
                        },
                        {
                            "Name": "event_time",
                            "Mapping": "$.event_time",
                            "SqlType": "TIMESTAMP"
                        },
                        {
                            "Name": "event_name",
                            "Mapping": "$.event_name",
                            "SqlType": "VARCHAR(8)"
                        },
                    ]
                }
            }
        ],
        "ApplicationCode": SQL,
    },
    "DependsOn": [
        kinesis_input_stream.title,
        kinesis_analytics_application_role.title,
    ]
}

kinesis_analytics_application_output_logic_id = "KinesisAnalyticsApplicationOutput"
kinesis_analytics_application_output_data = {
    "Type": "AWS::KinesisAnalytics::ApplicationOutput",
    "Properties": {
        "ApplicationName": kinesis_analytics_application_name,
        "Output": {
            "Name": OUTPUT_STREAM_NAME,
            "KinesisFirehoseOutput": {
                "ResourceARN": {"Fn::GetAtt": [kinesis_delivery_stream.title, "Arn"]},
                "RoleARN": {"Fn::GetAtt": [kinesis_analytics_application_role.title, "Arn"]},
            },
            "DestinationSchema": {
                "RecordFormatType": "JSON"
            }
        }
    },
    "DependsOn": [
        kinesis_delivery_stream.title,
        kinesis_analytics_application_role.title,
    ]
}


# Athena Table
glue_table_sign_up_counts = glue.Table(
    "GlueTableSignUpCounts",
    template=template,
    CatalogId=Ref(AWS_ACCOUNT_ID),
    DatabaseName="test",
    TableInput=glue.TableInput(
        Name="sign_up_counts",
        PartitionKeys=[
            glue.Column(Name="year", Type="smallint"),
            glue.Column(Name="month", Type="smallint"),
            glue.Column(Name="day", Type="smallint"),
            glue.Column(Name="hour", Type="smallint"),
            glue.Column(Name="minute", Type="smallint"),
        ],
        TableType="EXTERNAL_TABLE",
        StorageDescriptor=glue.StorageDescriptor(
            Columns=[
                glue.Column(Name="event_time", Type="timestamp"),
                glue.Column(Name="sign_up_event_counts", Type="smallint"),
            ],
            Compressed=False,
            SerdeInfo=glue.SerdeInfo(
                SerializationLibrary="org.openx.data.jsonserde.JsonSerDe",
                Parameters={"serialization.format": "1"}
            ),
            Location=helper_fn_sub("s3://eq-sanhe-for-everything/data/kinesis-analytics/{}", param_env_name),
            InputFormat="org.apache.hadoop.mapred.TextInputFormat",
            OutputFormat="org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat",
        ),
        Parameters={
            "EXTERNAL": "TRUE",
            "has_encrypted_data": "false",
        },
    )
)

template.add_parameter(param_env_name)

if __name__ == "__main__":
    import boto3

    AWS_PROFILE = "eq_sanhe"
    AWS_REGION = "us-east-1"
    ENV_NAME = "login-gov-metrics-dev"
    CFT_BUCKET = "eq-sanhe-for-everything"

    boto_ses = boto3.session.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    s3_client = boto_ses.client("s3")
    cf_client = boto_ses.client("cloudformation")

    template_data = template.to_dict()
    template_data["Resources"][kinesis_analytics_application_logic_id] = kinesis_analytics_application_data
    template_data["Resources"][
        kinesis_analytics_application_output_logic_id] = kinesis_analytics_application_output_data
    template_json = json.dumps(template_data, indent=4, sort_keys=True)
    template_url = upload_template(
        s3_client=s3_client,
        template_content=template_json,
        bucket_name=CFT_BUCKET,
    )
    deploy_stack(
        cf_client=cf_client,
        stack_name=ENV_NAME,
        template_url=template_url,
        stack_tags={
            "EnvironmentName": ENV_NAME,
        },
        stack_parameters={
            "EnvironmentName": ENV_NAME,
        },
        include_iam=True,
    )
