#!/usr/bin/env python3

from aws_cdk import core

from serverless_datalake.serverless_datalake_stack import ServerlessDatalakeStack

app = core.App()
ServerlessDatalakeStack(app, "ServerlessDatalake",
                        event_sources=['clicks', 'tweets'],
                        create_test_subscriber=True)
app.synth()
