#!/usr/bin/env node
import 'source-map-support/register';
import {App} from '@aws-cdk/core';
import {DynamodbStack} from '../lib/dynamodb-stack';

const app = new App();

new DynamodbStack(app, "DynamodbStack", {
    env: {
        account: process.env.CDK_DEFAULT_ACCOUNT,
        region: process.env.CDK_DEFAULT_REGION
    }
});
