import json

from aws_cdk import (
    aws_apigateway as _api_gtw,
    aws_dynamodb as _ddb,
    aws_iam as _iam,
    aws_kinesisfirehose as _kfh,
    aws_lambda as _lambda,
    aws_lambda_event_sources as _lambda_event_sources,
    aws_logs as _logs,
    aws_s3 as _s3,
    aws_s3_notifications as _s3_notifications,
    aws_sns as _sns,
    aws_sns_subscriptions as _sns_subscriptions,
    aws_sqs as _sqs,
    aws_ssm as _ssm,
    core
)


class ServerlessDatalakeStack(core.Stack):
    stack_id = None

    def __init__(self, scope: core.Construct, id: str, event_sources, create_test_subscriber=False, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.stack_id = id

        # Storage
        bucket, notification_queue = self.storage()
        # Catalog
        catalog_table, event_recorder = self.catalog(
            bucket, notification_queue)
        # Streams
        event_streams = self.event_streams(
            bucket, event_recorder, event_sources)
        # Replay
        event_replayer = self.replay(catalog_table, notification_queue)
        # REST API
        self.rest_api(event_streams, event_replayer)
        # Distribution
        distribution_topics = self.distribution_topics(
            event_recorder, event_sources)

        # Test Subscriber
        if create_test_subscriber:
            self.test_subscriber(distribution_topics)

    def storage(self):
        bucket = _s3.Bucket(
            self,
            "Bucket",
            removal_policy=core.RemovalPolicy.DESTROY
        )
        notification_queue = _sqs.Queue(
            self,
            "NotificationQueue",
        )
        bucket.add_object_created_notification(
            dest=_s3_notifications.SqsDestination(notification_queue)
        )
        return bucket, notification_queue

    def catalog(self, bucket, notification_queue):
        catalog_table = _ddb.Table(
            self,
            "CatalogTable",
            billing_mode=_ddb.BillingMode.PAY_PER_REQUEST,
            removal_policy=core.RemovalPolicy.DESTROY,
            partition_key=_ddb.Attribute(
                name='Source',
                type=_ddb.AttributeType.STRING
            ),
            sort_key=_ddb.Attribute(
                name='Timestamp',
                type=_ddb.AttributeType.STRING
            )
        )

        event_recorder = _lambda.Function(
            self,
            "EventRecorder",
            handler='lambda_function.lambda_handler',
            # https://github.com/aws/aws-cdk/issues/5491
            # pylint: disable=no-value-for-parameter
            code=_lambda.Code.asset('src/event_recorder'),
            runtime=_lambda.Runtime.PYTHON_3_7,
            log_retention=_logs.RetentionDays.ONE_MONTH,
            environment={
                'BUCKET_NAME': bucket.bucket_name,
                'TABLE_NAME': catalog_table.table_name,
                'TOPIC_SSM_PREFIX': "/{}/DistributionTopics/".format(self.stack_id)
            }
        )
        bucket.grant_read(event_recorder)
        catalog_table.grant_write_data(event_recorder)
        event_recorder.add_event_source(
            _lambda_event_sources.SqsEventSource(notification_queue)
        )
        return catalog_table, event_recorder

    def replay(self, catalog_table, notification_queue):
        event_replayer = _lambda.Function(
            self,
            "EventReplayer",
            handler='lambda_function.lambda_handler',
            # https://github.com/aws/aws-cdk/issues/5491
            # pylint: disable=no-value-for-parameter
            code=_lambda.Code.asset('src/event_replayer'),
            runtime=_lambda.Runtime.PYTHON_3_7,
            log_retention=_logs.RetentionDays.ONE_MONTH,
            environment={
                'TABLE_NAME': catalog_table.table_name,
                'QUEUE_URL': notification_queue.queue_url
            }
        )
        catalog_table.grant_read_data(event_replayer)
        notification_queue.grant_send_messages(event_replayer)
        return event_replayer

    def event_streams(self, bucket, event_recorder, event_sources):
        stream_role = _iam.Role(
            self,
            "StreamRole",
            assumed_by=_iam.ServicePrincipal('firehose.amazonaws.com')
        )
        bucket.grant_write(stream_role)

        event_streams = []
        for source in event_sources:
            event_streams.append(
                _kfh.CfnDeliveryStream(
                    self,
                    "{}Stream".format(source.capitalize()),
                    delivery_stream_name=source,
                    delivery_stream_type='DirectPut',
                    extended_s3_destination_configuration=_kfh.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                        bucket_arn=bucket.bucket_arn,
                        buffering_hints=_kfh.CfnDeliveryStream.BufferingHintsProperty(
                            interval_in_seconds=60,
                            size_in_m_bs=10
                        ),
                        compression_format='GZIP',
                        role_arn=stream_role.role_arn,
                        prefix="{}/".format(source)
                    )
                )
            )
        return event_streams

    def rest_api(self, event_streams, event_replayer):
        rest_api = _api_gtw.RestApi(
            self,
            "{}RestApi".format(self.stack_id)
        )
        rest_api.add_usage_plan(
            "RestApiUsagePlan",
            api_key=_api_gtw.ApiKey(
                self,
                "TestApiKey"
            ),
            api_stages=[
                _api_gtw.UsagePlanPerApiStage(
                    api=rest_api,
                    stage=rest_api.deployment_stage
                )
            ]
        )
        api_role = _iam.Role(
            self,
            "RestApiRole",
            assumed_by=_iam.ServicePrincipal('apigateway.amazonaws.com')
        )
        api_role.add_to_policy(_iam.PolicyStatement(
            actions=['firehose:PutRecord'],
            resources=[stream.attr_arn for stream in event_streams]
        ))
        for stream in event_streams:
            stream_resource = rest_api.root.add_resource(
                path_part=stream.delivery_stream_name.lower(),
            )
            stream_resource.add_method(
                'POST',
                api_key_required=True,
                integration=_api_gtw.Integration(
                    type=_api_gtw.IntegrationType.AWS,
                    uri="arn:aws:apigateway:eu-west-1:firehose:action/PutRecord",
                    integration_http_method='POST',
                    options=_api_gtw.IntegrationOptions(
                        credentials_role=api_role,
                        passthrough_behavior=_api_gtw.PassthroughBehavior.NEVER,
                        request_parameters={
                            'integration.request.header.Content-Type': "'application/x-amz-json-1.1'"
                        },
                        request_templates={
                            'application/json': json.dumps(
                                {
                                    "DeliveryStreamName": stream.delivery_stream_name,
                                    "Record": {
                                        "Data": "$util.base64Encode($input.body)"
                                    }
                                }
                            )
                        },
                        integration_responses=[
                            _api_gtw.IntegrationResponse(
                                status_code="200"
                            )
                        ]
                    )
                ),
                method_responses=[
                    _api_gtw.MethodResponse(
                        status_code="200"
                    )
                ]
            )
            replay = stream_resource.add_resource(
                path_part='replay'
            )
            replay.add_method(
                http_method='POST',
                integration=_api_gtw.LambdaIntegration(event_replayer),
                method_responses=[
                    _api_gtw.MethodResponse(
                        status_code="202"
                    ),
                    _api_gtw.MethodResponse(
                        status_code="400"
                    )
                ]
            )

    def distribution_topics(self, event_recorder, event_sources):
        distribution_topics = []
        for source in event_sources:
            topic = _sns.Topic(
                self,
                "{}DistributionTopic".format(source.capitalize())
            )
            _ssm.StringParameter(
                self,
                "{}DistributionParameter".format(source.capitalize()),
                parameter_name="/{}/DistributionTopics/{}".format(
                    self.stack_id, source),
                string_value=topic.topic_arn
            ).grant_read(event_recorder)
            topic.grant_publish(event_recorder)
            distribution_topics.append(topic)
        return distribution_topics

    def test_subscriber(self, distribution_topics):
        test_subscriber = _lambda.Function(
            self,
            "TestSubscriber",
            handler='lambda_function.lambda_handler',
            # https://github.com/aws/aws-cdk/issues/5491
            # pylint: disable=no-value-for-parameter
            code=_lambda.Code.asset('src/test_subscriber'),
            runtime=_lambda.Runtime.PYTHON_3_7,
            log_retention=_logs.RetentionDays.ONE_MONTH
        )
        for topic in distribution_topics:
            topic.add_subscription(
                _sns_subscriptions.LambdaSubscription(test_subscriber)
            )
