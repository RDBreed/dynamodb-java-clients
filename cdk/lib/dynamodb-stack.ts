import * as cdk from '@aws-cdk/core';
import * as dynamodb from '@aws-cdk/aws-dynamodb';
import {BillingMode, ProjectionType} from '@aws-cdk/aws-dynamodb';

export class DynamodbStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const tableName = this.node.tryGetContext('table_name');

    const table = new dynamodb.Table(this, tableName, {
      tableName: tableName,
      billingMode: BillingMode.PAY_PER_REQUEST,
      partitionKey: {name: 'id', type: dynamodb.AttributeType.STRING}
    });
    table.addGlobalSecondaryIndex({
      partitionKey: {name: 'lastName', type: dynamodb.AttributeType.STRING},
      indexName: "lastNameIndex",
      projectionType: ProjectionType.ALL
    });
  }
}
