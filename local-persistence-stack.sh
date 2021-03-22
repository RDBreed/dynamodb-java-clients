echo "Loading local aws environment"

aws configure set default.region eu-west-1
#go to cdk directory to run cdklocal
cd /cdk
CDK_DEFAULT_REGION=eu-west-1 CDK_DEFAULT_ACCOUNT=000000000000 cdklocal bootstrap --version-reporting false
CDK_DEFAULT_REGION=eu-west-1 CDK_DEFAULT_ACCOUNT=000000000000 cdklocal deploy --version-reporting false

echo "Waiting for dynamodb table 'user', attempting every 5s"
awslocal dynamodb wait --table-name table-exists user
echo 'Success: Reached dynamodb'

echo "Done loading"
