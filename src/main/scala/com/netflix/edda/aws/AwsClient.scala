/*
 * Copyright 2012-2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.edda.aws

import com.netflix.edda.Utils

import software.amazon.awssdk.core.auth.AwsCredentials
import software.amazon.awssdk.core.auth.AwsCredentialsProvider
import software.amazon.awssdk.core.auth.DefaultCredentialsProvider
import software.amazon.awssdk.core.auth.StaticCredentialsProvider
import software.amazon.awssdk.core.AwsRequestOverrideConfig
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.core.auth.ProfileCredentialsProvider

import software.amazon.awssdk.core.regions.Region
import software.amazon.awssdk.services.ec2.EC2Client
import software.amazon.awssdk.services.autoscaling.AutoScalingClient
import software.amazon.awssdk.services.elasticloadbalancing.ElasticLoadBalancingClient
import software.amazon.awssdk.services.elasticloadbalancingv2.ElasticLoadBalancingv2Client
import software.amazon.awssdk.services.iam.IAMClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.sqs.SQSClient
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient
import software.amazon.awssdk.services.route53.Route53Client
import software.amazon.awssdk.services.rds.RDSClient
import software.amazon.awssdk.services.elasticache.ElastiCacheClient
import software.amazon.awssdk.services.dynamodb.DynamoDBClient
import software.amazon.awssdk.services.cloudformation.CloudFormationClient
import software.amazon.awssdk.services.sts.STSClient
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

object AwsClient {
  def mkCredentialProvider(accessKey: String, secretKey: String, arn: String): AwsCredentialsProvider = {
    val provider = if( accessKey.isEmpty ) {
      DefaultCredentialsProvider.create()
    } else {
      StaticCredentialsProvider.create(AwsCredentials.create(accessKey, secretKey))
    }
    if (arn.isEmpty) {
        provider
    } else {
       StsAssumeRoleCredentialsProvider.builder()
        .refreshRequest(
          AssumeRoleRequest.builder()
            .roleArn(arn)
            .roleSessionName("edda")
            .requestOverrideConfig(
              AwsRequestOverrideConfig.builder().credentialsProvider(provider).build()
            )
            .build()
        )
        .build()
    }
  }
}


/** provides access to AWS service client objects
  *
  * @param credentials provider used to connect to AWS services
  * @param region used to select endpoint for AWS services
  */
class AwsClient(val provider: AwsCredentialsProvider, val region: Region) {

  var account = ""

  /** uses [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentials.html software.amazon.awssdk.auth.AWSCredentials]] to create AWSCredentialsProvider
    *
    * @param credentials used to connect to AWS services
    * @param region to select endpoint for AWS services
    */
  //def this(credentials: AwsCredentials, region: String) =
  //  this(StaticCredentialsProvider.create(credentials), region)

  /** create credentials from config file for account
    * @param account
    */
  def this(account: String) =
    this(
      AwsClient.mkCredentialProvider(
        Utils.getProperty("edda", "aws.accessKey", account, "").get,
        Utils.getProperty("edda", "aws.secretKey", account, "").get,
        Utils.getProperty("edda", "aws.assumeRoleArn", account, "").get
      ),
      Region.of(Utils.getProperty("edda", "region", account, "").get)
    )

  /** create credentials from config file for account with specific region
    * @param account
    * @param region
    */
  def this(account: String, region: String) = 
    this(
      AwsClient.mkCredentialProvider(
        Utils.getProperty("edda", "aws.accessKey", account, "").get,
        Utils.getProperty("edda", "aws.secretKey", account, "").get,
        Utils.getProperty("edda", "aws.assumeRoleArn", account, "").get
      ),
      Region.of(region)
    )

  /** create credential from provided arguments
    *
    * @param accessKey for account access
    * @param secretKey for account access
    * @param region used to select endpoint for AWS service
    */
  def this(accessKey: String, secretKey: String, region: String) =
    this(AwsClient.mkCredentialProvider(accessKey,secretKey, ""), Region.of(region))


  /** generate a resource arn */
  def arn(resourceAPI: String, resourceType: String, resourceName: String): String = {
    "arn:aws:" + resourceAPI + ":" + region + ":" + account + ":" + resourceType + arnSeperator(resourceType) + resourceName
  }

  def arnSeperator(t:String): String = if (t == "loadbalancer") "/" else ":"

  def getAccountNum(): String = {
    var stsClient = STSClient.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
    stsClient.getCallerIdentity(GetCallerIdentityRequest.builder().build()).account()
  }

  def loadAccountNum() {
    this.setAccountNum(this.getAccountNum)
  }

  def setAccountNum(accountNumber: String) {
    this.account = accountNumber
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/ec2/EC2Client.html software.amazon.awssdk.services.ec2.EC2Client]] object */
  def ec2 = {
    val client = EC2Client.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/autoscaling/AutoScalingClient.html software.amazon.awssdk.services.autoscaling.AutoScalingClient]] object */
  def asg = {
    val client = AutoScalingClient.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticloadbalancing/ElasticLoadBalancingClient.html software.amazon.awssdk.services.elasticloadbalancing.ElasticLoadBalancingClient]] object */
  def elb = {
    val client = ElasticLoadBalancingClient.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticloadbalancingv2/ElasticLoadBalancingClient.html software.amazon.awssdk.services.elasticloadbalancingv2.ElasticLoadBalancingClient]] object */
  def elbv2 = {
    val client = ElasticLoadBalancingv2Client.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/S3Client.html software.amazon.awssdk.services.s3.S3Client]] object */
  def s3 = {
    val client = S3Client.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/identitymanagement/IdentityManagementClient.html software.amazon.awssdk.services.identitymanagement.IdentityManagementClient]] object */
  def identitymanagement = {
    val client = IAMClient.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/SQSClient.html software.amazon.awssdk.services.sqs.SQSClient]] object */
  def sqs = {
    val client = SQSClient.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
    client
  }

  /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/cloudwatch/CloudWatchClient.html software.amazon.awssdk.services.cloudwatch.CloudWatchClient]] object */
  def cw = {
    val client = CloudWatchClient.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
    client
  }

   /** get [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/route53/Route53Client.html software.amazon.awssdk.services.route53.Route53Client]] object */
   def route53 = {
      val client = Route53Client.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
      client
   }

   def rds = {
     val client = RDSClient.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
     client
   }

   def elasticache = {
    val client = ElastiCacheClient.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
    client
   }

   def dynamo = {
     val client = DynamoDBClient.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
     client
   }

   def cloudformation = {
    val client = CloudFormationClient.builder()
                    .region(region)
                    .credentialsProvider(provider)
                    .build();
    client
   }
}
