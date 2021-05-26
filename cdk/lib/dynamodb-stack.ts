import {Construct, Stack, StackProps} from '@aws-cdk/core';
import {AttributeType, BillingMode, ProjectionType, Table} from '@aws-cdk/aws-dynamodb';

export class DynamodbStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const tableName = this.node.tryGetContext('table_name');

    const table = new Table(this, tableName, {
      tableName: tableName,
      billingMode: BillingMode.PAY_PER_REQUEST,
      partitionKey: {name: 'id', type: AttributeType.STRING}
    });
    table.addGlobalSecondaryIndex({
      partitionKey: {name: 'lastName', type: AttributeType.STRING},
      indexName: "lastNameIndex",
      projectionType: ProjectionType.ALL
    });
  }
}
