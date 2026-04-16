#!/bin/bash

set -e

STACK_NAME="rps-pipeline-stack"
BUCKET_NAME="rps-temp-templates"
REGION="${1:-us-east-1}"
LAMBDA_S3_KEY="artifacts/lambda-code.zip"
S3_PREFIX="artifacts"

echo "Region: $REGION"
echo "Using bucket: $BUCKET_NAME"

if ! aws s3 ls "s3://$BUCKET_NAME" --region "$REGION" >/dev/null 2>&1; then
  echo "Creating bucket $BUCKET_NAME"
  if [ "$REGION" = "us-east-1" ]; then
    aws s3 mb "s3://$BUCKET_NAME"
  else
    aws s3 mb "s3://$BUCKET_NAME" --region "$REGION"
  fi
fi

echo "Packaging and uploading Lambda code"
cd components/lambda
zip -r lambda.zip user_actions.py
aws s3 cp lambda.zip "s3://$BUCKET_NAME/$LAMBDA_S3_KEY" --region "$REGION"
rm lambda.zip
cd ../..

GLUE_RUN_LAMBDA_S3_KEY="artifacts/glue-lambda-code.zip"

echo "Packaging and uploading Run glue job Lambda code"
cd components/lambda
zip -r glue-lambda-code.zip Trigger_GlueJob.py
aws s3 cp lambda.zip "s3://$BUCKET_NAME/$GLUE_RUN_LAMBDA_S3_KEY" --region "$REGION"
rm glue-lambda-code.zip
cd ../..

echo "Uploading Glue script to S3"
aws s3 cp components/glue/scripts/rps_glue_code.py "s3://$BUCKET_NAME/artifacts/scripts/rps_glue_code.py" --region "$REGION"

echo "Packaging CloudFormation template"
aws cloudformation package \
  --template-file Rock_Paper_Scissors.yaml \
  --s3-bucket "$BUCKET_NAME" \
  --s3-prefix "$S3_PREFIX" \
  --output-template-file packaged.yaml \
  --region "$REGION"
# checking if the stack already exists, if exists, update it, otherwise create it. checking stack status to determine if it exists or not

if aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" >/dev/null 2>&1; then
  echo "Updating CloudFormation stack"
  aws cloudformation update-stack \
    --stack-name "$STACK_NAME" \
    --template-body file://packaged.yaml \
    --parameters file://stack-param-files/param.json \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "$REGION"
else
# If the stack does not exist, creating it
  echo "Deploying CloudFormation stack"
  aws cloudformation create-stack \
    --stack-name "$STACK_NAME" \
    --template-body file://packaged.yaml \
    --parameters file://stack-param-files/param.json \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "$REGION"
fi
rm packaged.yaml
echo "Done. Check stack status with: aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION"
