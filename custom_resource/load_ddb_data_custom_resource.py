from aws_cdk import (
    aws_cloudformation as cfn,
    aws_lambda as lambda_,
    aws_iam as iam,
    core
)


class LoadDDBDataCustomResource(core.Construct):
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id)

        table_name = kwargs.get("table_name")
        table_arn = kwargs.get("table_arn")

        function = lambda_.SingletonFunction(
            self, "Singleton",
            uuid="22fbda4b-ee9f-4317-9489-c118134d8e97",
            code=lambda_.Code.asset("./lambda/load-ddb-data.zip"),
            handler="lambda_function.handler",
            timeout=core.Duration.seconds(300),
            runtime=lambda_.Runtime.PYTHON_3_8,
            environment={'TABLE_NAME': table_name}
        )

        function.add_to_role_policy(
            iam.PolicyStatement(actions=['dynamodb:PutItem'], resources=["*"],
                                effect=iam.Effect.ALLOW))

        resource = cfn.CustomResource(
            self, "Resource",
            provider=cfn.CustomResourceProvider.lambda_(function),
            properties=kwargs,
        )

        self.response = resource.get_att("Response").to_string()
