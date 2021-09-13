from aws_cdk import (
    aws_iot as _iot,
    aws_s3 as s3,
    aws_iam as iam,
    aws_elasticsearch as _elasticsearch,
    aws_lambda as _lambda,
    aws_kinesisfirehose as _firehose,
    aws_cognito,
    aws_dynamodb,
    aws_s3_notifications,
    core)

import random
import string
from custom_resource.load_es_index_custom_resource import LoadESIndexCustomResource
from custom_resource.load_kibana_dashboards_custom_resource import LoadKibanaDashboardsCustomResource
from custom_resource.load_ddb_data_custom_resource import LoadDDBDataCustomResource

DOMAIN = _elasticsearch.CfnDomain
RULE = _iot.CfnTopicRule

class IotKinesisElasticsearchStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, props, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        user_pool = aws_cognito.UserPool(self, 'kibanaUserPool', self_sign_up_enabled=False,
                                         sign_in_aliases=aws_cognito.SignInAliases(username=True, email=True), removal_policy=core.RemovalPolicy.DESTROY)

        cognito_domain = aws_cognito.CognitoDomainOptions(
            domain_prefix=f"{props['projectName'].lower()}-{''.join(random.choices(string.ascii_lowercase + string.digits, k=6))}")

        aws_cognito.UserPoolDomain(
            self, 'userPoolDomain', user_pool=user_pool, cognito_domain=cognito_domain)

        user_pool_client = aws_cognito.UserPoolClient(
            self, 'kibanaClientId', user_pool=user_pool, generate_secret=True)

        email = aws_cognito.CfnUserPoolUser.AttributeTypeProperty(
            name='email', value=self.node.try_get_context("cognito_user_email"))

        email_verified = aws_cognito.CfnUserPoolUser.AttributeTypeProperty(
            name='email_verified', value='True')

        user = aws_cognito.CfnUserPoolUser(
            self, 'admin', user_pool_id=user_pool.user_pool_id, username='admin', user_attributes=[email, email_verified])

        identity_provider = aws_cognito.CfnIdentityPool.CognitoIdentityProviderProperty(
            client_id=user_pool_client.user_pool_client_id, provider_name=user_pool.user_pool_provider_name)

        identity_pool = aws_cognito.CfnIdentityPool(self, 'identityPool',
                                                    allow_unauthenticated_identities=False,
                                                    cognito_identity_providers=[identity_provider])

        es_domain_arn = f"arn:aws:es:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:domain/{props['projectName'].lower()}"

        cognito_authenticated_role = iam.Role(self, "CognitoAuthRole",
                                              assumed_by=iam.FederatedPrincipal("cognito-identity.amazonaws.com",
                                                                                assume_role_action='sts:AssumeRoleWithWebIdentity',
                                                                                conditions={'StringEquals': {
                                                                                    'cognito-identity.amazonaws.com:aud': identity_pool.ref},
                                                                                    'ForAnyValue:StringLike': {
                                                                                        'cognito-identity.amazonaws.com:amr': 'authenticated'}}
                                                                                ))

        cognito_authenticated_role.add_to_policy(iam.PolicyStatement(
            actions=["es:ESHttp*"], resources=[f"{es_domain_arn}/*"], effect=iam.Effect.ALLOW))

        aws_cognito.CfnIdentityPoolRoleAttachment(self, 'identityPoolRoleAttachment',
                                                  identity_pool_id=identity_pool.ref,
                                                  roles={'authenticated': cognito_authenticated_role.role_arn})

        es_policy_statement = iam.PolicyStatement(actions=['es:*'],
                                                  resources=[f"{es_domain_arn}/*"])

        es_policy_statement.add_arn_principal(
            cognito_authenticated_role.role_arn)

        es_role = iam.Role(self, "CognitoAccessForAmazonESKibana",
                           assumed_by=iam.ServicePrincipal("es.amazonaws.com"),
                           managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name(
                               'AmazonESCognitoAccess')]
                           )

        cognito = _elasticsearch.CognitoOptions(user_pool_id=user_pool.user_pool_id,
                                                identity_pool_id=identity_pool.ref,
                                                role=es_role)

        domain = _elasticsearch.Domain(self, 'elasticsearch',
                                       domain_name=f"{props['projectName'].lower()}",
                                       version=_elasticsearch.ElasticsearchVersion.V6_8,
                                       access_policies=[es_policy_statement],
                                       cognito_kibana_auth=cognito,
                                       removal_policy=core.RemovalPolicy.DESTROY)

        power_transformers = aws_dynamodb.Table(
            self, "PowerTransformers",
            table_name="PowerTransformers",
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            partition_key=aws_dynamodb.Attribute(
                name="name",
                type=aws_dynamodb.AttributeType.STRING
            ),
            removal_policy=core.RemovalPolicy.DESTROY
        )

        function = _lambda.Function(self, "power_transformers_data_enrichment",
                                    function_name="power_transformers_data_enrichment",
                                    runtime=_lambda.Runtime.PYTHON_3_8,
                                    handler="lambda_function.handler",
                                    timeout=core.Duration.minutes(5),
                                    code=_lambda.Code.asset("./lambda/data-enrichment"))

        function.add_environment('TABLE_NAME', power_transformers.table_name)
        function.add_to_role_policy(
            iam.PolicyStatement(actions=['dynamodb:GetItem'], resources=[f"{power_transformers.table_arn}"],
                                effect=iam.Effect.ALLOW))

        raw_bucket = s3.Bucket(self, 'PowerTransformersRawDataBucket',
                               bucket_name=f"{props['projectName'].lower()}-raw-{core.Aws.ACCOUNT_ID}",
                               removal_policy=core.RemovalPolicy.DESTROY)

        delivery_stream_name = f"{props['projectName'].lower()}_stream"
        index_name = 'power-transformer'
        type_name = '_doc'

        firehose_role = iam.Role(self, "FirehoseRole",
                                 assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"))

        firehose_role.add_to_policy(iam.PolicyStatement(actions=["s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads", "s3:AbortMultipartUpload"],
                                                        resources=[raw_bucket.bucket_arn, raw_bucket.arn_for_objects('*')], effect=iam.Effect.ALLOW))

        firehose_role.add_to_policy(iam.PolicyStatement(actions=["es:DescribeElasticsearchDomain", "es:DescribeElasticsearchDomains", "es:DescribeElasticsearchDomainConfig", "es:ESHttpPost", "es:ESHttpPut"],
                                                        resources=[domain.domain_arn, f"{domain.domain_arn}/*"], effect=iam.Effect.ALLOW))

        firehose_role.add_to_policy(iam.PolicyStatement(actions=["es:ESHttpGet"],
                                                        resources=[f"{domain.domain_arn}/_all/_settings", f"{domain.domain_arn}/_cluster/stats",
                                                        f"{domain.domain_arn}/{index_name}*/_mapping/{type_name}",
                                                                   f"{domain.domain_arn}/_nodes", f"{domain.domain_arn}/_nodes/stats",
                                                                   f"{domain.domain_arn}/_nodes/*/stats", f"{domain.domain_arn}/_stats", f"{domain.domain_arn}/{index_name}*/_stats"], effect=iam.Effect.ALLOW))

        firehose_role.add_to_policy(iam.PolicyStatement(actions=["lambda:InvokeFunction", "lambda:GetFunctionConfiguration"],
                                                        resources=[f"{function.function_arn}:$LATEST"], effect=iam.Effect.ALLOW))

        firehose_role.add_to_policy(iam.PolicyStatement(actions=["logs:PutLogEvents"],
                                                        resources=[f"arn:aws:log:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:log-group:/aws/kinesisfirehose/{props['projectName'].lower()}_stream:log-stream:*"], effect=iam.Effect.ALLOW))

        s3_configuration = _firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
            bucket_arn=raw_bucket.bucket_arn, role_arn=firehose_role.role_arn)
        buffering_hints = _firehose.CfnDeliveryStream.ElasticsearchBufferingHintsProperty(
            interval_in_seconds=60, size_in_m_bs=1)
        lambda_parameter = _firehose.CfnDeliveryStream.ProcessorParameterProperty(
            parameter_name='LambdaArn', parameter_value=f"{function.function_arn}:$LATEST")
        lambda_buffer_size = _firehose.CfnDeliveryStream.ProcessorParameterProperty(
            parameter_name='BufferSizeInMBs', parameter_value='1')
            
        lambda_processor = _firehose.CfnDeliveryStream.ProcessorProperty(
            type='Lambda', parameters=[lambda_parameter])
        procesing_configuration = _firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
            enabled=True, processors=[lambda_processor])

        es_configuration = _firehose.CfnDeliveryStream.ElasticsearchDestinationConfigurationProperty(index_name=index_name, role_arn=firehose_role.role_arn,
                                                                                                     s3_configuration=s3_configuration, buffering_hints=buffering_hints, domain_arn=domain.domain_arn,
                                                                                                     s3_backup_mode='AllDocuments', processing_configuration=procesing_configuration,
                                                                                                     index_rotation_period='NoRotation', type_name=type_name)

        load_es_index_custom_resource = LoadESIndexCustomResource(self, "LoadESIndex",
                                                          es_host=domain.domain_endpoint,
                                                          es_region=f"{core.Aws.REGION}",
                                                          es_domain_arn=f"{es_domain_arn}/*")
        
        delivery_stream = _firehose.CfnDeliveryStream(self, 'FirehoseES', delivery_stream_name=delivery_stream_name,
                                                      elasticsearch_destination_configuration=es_configuration)
                                                      
        delivery_stream.node.add_dependency(load_es_index_custom_resource)

        iot_topic_rule_role = iam.Role(self, "IotTopicRuleRole",
                                       assumed_by=iam.ServicePrincipal("iot.amazonaws.com"))

        iot_topic_rule_role.add_to_policy(iam.PolicyStatement(actions=["firehose:PutRecord"],
                                                              resources=[f"arn:aws:firehose:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:deliverystream/{delivery_stream_name}"], effect=iam.Effect.ALLOW))

        firehose_action = RULE.ActionProperty(firehose=RULE.FirehoseActionProperty(
            delivery_stream_name=delivery_stream_name, role_arn=iot_topic_rule_role.role_arn))

        topic_rule_payload = RULE.TopicRulePayloadProperty(
            actions=[firehose_action], sql="SELECT * FROM 'pt/telemetry'", rule_disabled=False)

        rule = RULE(self, 'iot_rule', rule_name='pt_iot_kinesis',
                    topic_rule_payload=topic_rule_payload)

        load_ddb_custom_resource = LoadDDBDataCustomResource(self, "LoadDDBData",
                                                             table_name=power_transformers.table_name,
                                                             table_arn=power_transformers.table_arn)

        load_kibana_dashboards_custom_resource = LoadKibanaDashboardsCustomResource(self, "LoadKibanaDashboards",
                                                                                    es_host=domain.domain_endpoint,
                                                                                    es_region=f"{core.Aws.REGION}",
                                                                                    es_domain_arn=f"{es_domain_arn}/*")
