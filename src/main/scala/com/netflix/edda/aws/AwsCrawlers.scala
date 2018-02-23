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

import scala.actors.Actor
import scala.collection.mutable.ListBuffer
import com.netflix.edda.StateMachine
import com.netflix.edda.Crawler
import com.netflix.edda.CrawlerState
import com.netflix.edda.Observable
import com.netflix.edda.Record
import com.netflix.edda.RecordSet
import com.netflix.edda.RequestId
import com.netflix.edda.BeanMapper
import com.netflix.edda.basic.BasicBeanMapper
import com.netflix.edda.Utils
import com.netflix.edda.ObserverExecutionContext
import java.time.Instant
import software.amazon.awssdk.core.exception.SdkServiceException
import software.amazon.awssdk.services.ec2.model.DescribeAddressesRequest
import software.amazon.awssdk.services.ec2.model.DescribeImagesRequest
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest
import software.amazon.awssdk.services.ec2.model.DescribeNetworkInterfacesRequest
import software.amazon.awssdk.services.ec2.model.DescribeReservedInstancesRequest
import software.amazon.awssdk.services.ec2.model.DescribeReservedInstancesOfferingsRequest
import software.amazon.awssdk.services.ec2.model.DescribeSecurityGroupsRequest
import software.amazon.awssdk.services.ec2.model.DescribeSnapshotsRequest
import software.amazon.awssdk.services.ec2.model.DescribeSubnetsRequest
import software.amazon.awssdk.services.ec2.model.DescribeTagsRequest
import software.amazon.awssdk.services.ec2.model.DescribeVolumesRequest
import software.amazon.awssdk.services.ec2.model.DescribeVpcsRequest
import software.amazon.awssdk.services.iam.model._
import software.amazon.awssdk.services.s3.model.ListBucketsRequest
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest

import software.amazon.awssdk.services.cloudformation.model.DescribeStacksRequest
import software.amazon.awssdk.services.cloudformation.model.ListStackResourcesRequest

import software.amazon.awssdk.services.cloudwatch.model.DescribeAlarmsRequest
import software.amazon.awssdk.services.autoscaling.model.DescribeAutoScalingGroupsRequest
import software.amazon.awssdk.services.autoscaling.model.DescribeLaunchConfigurationsRequest
import software.amazon.awssdk.services.autoscaling.model.DescribePoliciesRequest
import software.amazon.awssdk.services.autoscaling.model.DescribeScalingActivitiesRequest
import software.amazon.awssdk.services.autoscaling.model.DescribeScheduledActionsRequest
import software.amazon.awssdk.services.elasticloadbalancing.model.DescribeLoadBalancersRequest
import software.amazon.awssdk.services.elasticloadbalancing.model.DescribeInstanceHealthRequest
import software.amazon.awssdk.services.elasticloadbalancingv2.model.DescribeListenersRequest
import software.amazon.awssdk.services.elasticloadbalancingv2.model.{DescribeLoadBalancersRequest => DescribeLoadBalancersV2Request}
import software.amazon.awssdk.services.elasticloadbalancingv2.model.DescribeTargetGroupsRequest
import software.amazon.awssdk.services.route53.model.ListHostedZonesRequest
import software.amazon.awssdk.services.route53.model.ListResourceRecordSetsRequest

import software.amazon.awssdk.services.elasticache.model.DescribeCacheClustersRequest

import software.amazon.awssdk.services.rds.model.DescribeDBInstancesRequest
import software.amazon.awssdk.services.rds.model.DescribeDBSubnetGroupsRequest
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest

import collection.JavaConverters._
import java.util.concurrent.Executors
import java.util.concurrent.Callable

import org.slf4j.LoggerFactory

import software.amazon.awssdk.services.rds.model.DescribeDBInstancesRequest
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest
import software.amazon.awssdk.services.elasticache.model.DescribeCacheClustersRequest
import software.amazon.awssdk.services.cloudformation.model.DescribeStacksRequest
import software.amazon.awssdk.services.cloudformation.model.ListStackResourcesRequest
import software.amazon.awssdk.services.cloudformation.model.Stack

/** static namespace for out Context trait */
object AwsCrawler {

  /** All AWS Crawlers require the basic ConfigContext
    * plus an [[com.netflix.edda.aws.AwsClient]] and [[com.netflix.edda.BeanMapper]]
    */
  trait Context {
    def awsClient: AwsClient

    def beanMapper: BeanMapper
  }

}

/** specialized [[com.netflix.edda.BeanMapper]] trait that can suppress specific AWS resource tags
  * based on patterns expressed in the config.  This is necessary in case people add tags to
  * resources that change frequenly (like timestamps).  The AwsBeanMapper trait also works around
  * some internal state exposed in the bean for [[http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/ec2/model/InstanceState.html software.amazon.awssdk.services.ec2.model.InstanceState]]
  * *code* field.
  */
trait AwsBeanMapper extends BeanMapper {
  val basicBeanMapper = new BasicBeanMapper
  val suppressSet = Utils.getProperty("edda.crawler", "aws.suppressTags", "", "").get.split(",").toSet

  val suppressKeyMapper: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
    case (obj: software.amazon.awssdk.services.ec2.model.Tag, "value", Some(x: Any)) if suppressSet.contains(obj.key()) => Some("[EDDA_SUPPRESSED]")
    case (obj: software.amazon.awssdk.services.ec2.model.TagDescription, "value", Some(x: Any)) if suppressSet.contains(obj.key()) => Some("[EDDA_SUPPRESSED]")
    case (obj: software.amazon.awssdk.services.autoscaling.model.Tag, "value", Some(x: Any)) if suppressSet.contains(obj.key()) => Some("[EDDA_SUPPRESSED]")
    case (obj: software.amazon.awssdk.services.autoscaling.model.TagDescription, "value", Some(x: Any)) if suppressSet.contains(obj.key()) => Some("[EDDA_SUPPRESSED]")
  }
  basicBeanMapper.addKeyMapper(suppressKeyMapper)

  def flattenTag(obj: Map[String,Any]) = obj + (obj("key").asInstanceOf[String] -> obj("value"))

  // this will flatten the tags so that we will have: { key -> a, value -> b, a -> b }
  val tagObjMapper: PartialFunction[AnyRef,AnyRef] = {
    case obj : software.amazon.awssdk.services.ec2.model.Tag =>
      flattenTag(basicBeanMapper.fromBean(obj).asInstanceOf[Map[String,Any]])
    case obj : software.amazon.awssdk.services.ec2.model.TagDescription =>
      flattenTag(basicBeanMapper.fromBean(obj).asInstanceOf[Map[String,Any]])
    case obj : software.amazon.awssdk.services.autoscaling.model.Tag =>
      flattenTag(basicBeanMapper.fromBean(obj).asInstanceOf[Map[String,Any]])
    case obj : software.amazon.awssdk.services.autoscaling.model.TagDescription =>
      flattenTag(basicBeanMapper.fromBean(obj).asInstanceOf[Map[String,Any]])
    case obj : software.amazon.awssdk.services.elasticloadbalancing.model.Tag =>
      flattenTag(basicBeanMapper.fromBean(obj).asInstanceOf[Map[String,Any]])
    case obj : software.amazon.awssdk.services.rds.model.Tag =>
      flattenTag(basicBeanMapper.fromBean(obj).asInstanceOf[Map[String,Any]])
  }
  this.addObjMapper(tagObjMapper)

  val instanceStateKeyMapper: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
    case (obj: software.amazon.awssdk.services.ec2.model.InstanceState, "code", Some(value: Int)) => Some(0x00FF & value)
  }
  this.addKeyMapper(instanceStateKeyMapper)

}

/** iterator interface for working with the paginated results from some
  * aws apis
  */
abstract class AwsIterator extends Iterator[Seq[Record]] {
  var nextToken: Option[String] = Some(null)

  def hasNext = nextToken != None

  def next(): List[Record]
}

/** crawler for EIP Addresses
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsAddressCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = DescribeAddressesRequest.builder().build()

  override def doCrawl()(implicit req: RequestId) =
    backoffRequest { ctx.awsClient.ec2.describeAddresses(request).addresses.asScala.map(
      item => Record(item.publicIp(), ctx.beanMapper(item))) }.toSeq
}

/** crawler for AutoScalingGroups
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsAutoScalingGroupCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = DescribeAutoScalingGroupsRequest.builder()
    .maxRecords(50)
    .build()

  lazy val abortWithoutTags = Utils.getProperty("edda.crawler", "abortWithoutTags", name, "false")
  override def doCrawl()(implicit req: RequestId) = {
    var tagCount = 0
    val it = new AwsIterator() {
      def next() = {
        val response = backoffRequest { ctx.awsClient.asg.describeAutoScalingGroups(request.toBuilder().nextToken(this.nextToken.get).build()) }
        this.nextToken = Option(response.nextToken())
        response.autoScalingGroups().asScala.map(
          item => {
            tagCount += item.tags().size
            Record(item.autoScalingGroupName(), item.createdTime(), ctx.beanMapper(item))
          }).toList
      }
    }
    val list = it.toList.flatten
    if (tagCount == 0) {
      if (abortWithoutTags.get.toBoolean) {
        throw new java.lang.RuntimeException("no tags found for any record in " + name + ", ignoring crawl results")
      } else if (logger.isWarnEnabled) logger.warn(s"$req no tags found for any record in $name.  If you expect at least one tag then set: edda.crawler.$name.abortWithoutTags=true")
    }
    list
  }
}

/** crawler for ASG Policies
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsScalingPolicyCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = DescribePoliciesRequest.builder()
    .maxRecords(50)
    .build()

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator() {
      def next() = {
        val response = backoffRequest { ctx.awsClient.asg.describePolicies(request.toBuilder().nextToken(this.nextToken.get).build()) }
        this.nextToken = Option(response.nextToken())
        response.scalingPolicies().asScala.map(
          item => {
            Record(item.policyName(), ctx.beanMapper(item))
          }).toList
      }
    }
    it.toList.flatten
  }
}

/** crawler for ASG Activities
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsScalingActivitiesCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = DescribeScalingActivitiesRequest.builder()
    .maxRecords(50)
    .build()

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator() {
      def next() = {
        val response = backoffRequest { ctx.awsClient.asg.describeScalingActivities(request.toBuilder().nextToken(this.nextToken.get).build()) }
        this.nextToken = Option(response.nextToken())
        response.activities().asScala.map(
          item => {
            Record(item.activityId(), ctx.beanMapper(item))
          }).toList
      }
    }
    it.toList.flatten
  }
}

/** crawler for ASG Scheduled Actions
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsScheduledActionsCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = DescribeScheduledActionsRequest.builder()
    .maxRecords(50)
    .build()

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator() {
      def next() = {
        val response = backoffRequest { ctx.awsClient.asg.describeScheduledActions(request.toBuilder().nextToken(this.nextToken.get).build()) }
        this.nextToken = Option(response.nextToken())
        response.scheduledUpdateGroupActions().asScala.map(
          item => {
            Record(item.scheduledActionARN(), ctx.beanMapper(item))
          }).toList
      }
    }
    it.toList.flatten
  }
}

/** crawler for ASG VPCs
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsVpcCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = DescribeVpcsRequest.builder().build()

  override def doCrawl()(implicit req: RequestId) = {
    val response = backoffRequest { ctx.awsClient.ec2.describeVpcs() }
    response.vpcs().asScala.map(
      item => {
        Record(item.vpcId(), ctx.beanMapper(item))
      }).toList
  }
}


/** crawler for CloudWatch Alarms
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsAlarmCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = DescribeAlarmsRequest.builder()
    .maxRecords(100)
    .build()

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator() {
      def next() = {
        val response = backoffRequest { ctx.awsClient.cw.describeAlarms(request.toBuilder().nextToken(this.nextToken.get).build()) }
        this.nextToken = Option(response.nextToken())
        response.metricAlarms().asScala.map(
          item => {
            Record(item.alarmName(), ctx.beanMapper(item))
          }).toList
      }
    }
    it.toList.flatten
  }
}

/** crawler for Images
  *
  * if tags are used with on the aws images objects, set edda.crawler.NAME.abortWithoutTags=true
  * so that the crawler can detect when AWS sends a degraded result without tag information.
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsImageCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = DescribeImagesRequest.builder().build()
  lazy val abortWithoutTags = Utils.getProperty("edda.crawler", "abortWithoutTags", name, "false")

  override def doCrawl()(implicit req: RequestId) = {
    var tagCount = 0
    val list = backoffRequest { ctx.awsClient.ec2.describeImages(request).images() }.asScala.map(
      item => {
        tagCount += item.tags().size
        Record(item.imageId(), ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && abortWithoutTags.get.toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

/** crawler for LoadBalancers
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsLoadBalancerCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = DescribeLoadBalancersRequest.builder()
    .pageSize(400)
    .build()

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator() {
      def next() = {
        val response = backoffRequest { ctx.awsClient.elb.describeLoadBalancers(request.toBuilder().marker(this.nextToken.get).build()) }
        this.nextToken = Option(response.nextMarker())
        response.loadBalancerDescriptions().asScala.map(
          item => {
            Record(item.loadBalancerName(), item.createdTime(), ctx.beanMapper(item))
          }).toList
      }
    }

    // List[Seq[Record]]
    var initial = it.toSeq.flatten.grouped(20).toList
    backoffRequest { ctx.awsClient.loadAccountNum() }

    var buffer = new ListBuffer[Record]()

    for (group <- initial) {
      var names = new ListBuffer[String]()

      for (rec <- group) {
        val data = rec.toMap("data").asInstanceOf[Map[String,String]]
        names += data("loadBalancerName")
      }
      try {
        val request = software.amazon.awssdk.services.elasticloadbalancing.model.DescribeTagsRequest.builder().loadBalancerNames(names.asJava).build()
        val response = backoffRequest { ctx.awsClient.elb.describeTags(request) }
        val responseList = backoffRequest { response.tagDescriptions().asScala.map(
          item => {
            ctx.beanMapper(item)
          }).toSeq
        }

        for (rec <- group) {
          val data = rec.toMap("data").asInstanceOf[Map[String,String]]
          for (response <- responseList) {
            if (response.asInstanceOf[Map[String,Any]]("loadBalancerName") == data("loadBalancerName")) {
              buffer += rec.copy(data = data.asInstanceOf[Map[String,Any]] ++ Map("tags" -> response.asInstanceOf[Map[String,Any]]("tags")))
            }
          }
        }
      } catch {
        case e: Exception => {
          logger.error("error retrieving tags for an elb", e)
        }
      }
    }
    buffer.toList
  }
}

/** crawler for LoadBalancers (version 2)
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsLoadBalancerV2Crawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = DescribeLoadBalancersV2Request.builder().build()

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator {
      override def next() = {
        val response = backoffRequest {
          ctx.awsClient.elbv2.describeLoadBalancers(request.toBuilder().marker(this.nextToken.get).build())
        }
        this.nextToken = Option(response.nextMarker)
        response.loadBalancers.asScala.map(
          item => {
            val lr = DescribeListenersRequest.builder().loadBalancerArn(item.loadBalancerArn).build()
            val listeners = backoffRequest { ctx.awsClient.elbv2.describeListeners(lr) }.listeners
            // If there are no listeners AWS returns null instead of an empty list
            val listenersList = if (listeners == null) Nil else listeners.asScala.map(item => ctx.beanMapper(item)).toList
            val data = ctx.beanMapper(item).asInstanceOf[Map[String, Any]] ++ Map("listeners" -> listenersList)

            Record(item.loadBalancerName, item.createdTime, data)
          }
        ).toList
      }
    }
    it.toList.flatten
  }
}

/** crawler for TargetGroups
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsTargetGroupCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = DescribeTargetGroupsRequest.builder().build()

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator {
      override def next() = {
        val response = backoffRequest {
          ctx.awsClient.elbv2.describeTargetGroups(request.toBuilder().marker(this.nextToken.get).build())
        }
        this.nextToken = Option(response.nextMarker)
        response.targetGroups.asScala.map(
          item => Record(item.targetGroupName, ctx.beanMapper(item))
        ).toList
      }
    }
    it.toList.flatten
  }
}

case class AwsInstanceHealthCrawlerState(elbRecords: Seq[Record] = Seq[Record]())

object AwsInstanceHealthCrawler extends StateMachine.LocalState[AwsInstanceHealthCrawlerState]

/** crawler for LoadBalancer Instances
  *
  * this is a secondary crawler that takes the results from the AwsLoadBalancerCrawler
  * and then crawls the instance states for each ELB.
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  * @param crawler the LoadBalancer crawler
  */
class AwsInstanceHealthCrawler(val name: String, val ctx: AwsCrawler.Context, val crawler: Crawler) extends Crawler {

  import java.util.concurrent.TimeUnit

  import com.netflix.servo.monitor.Monitors
  import com.netflix.servo.DefaultMonitorRegistry

  val crawlTimer = Monitors.newTimer("crawl")

  import AwsInstanceHealthCrawler._

  override def crawl()(implicit req: RequestId) {}

  // we don't crawl, just get updates from crawler when it crawls
  override def doCrawl()(implicit req: RequestId) = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on InstanceHealthCrawler")

  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(10)

  /** for each elb call describeInstanceHealth and map that to a new document
    *
    * @param elbRecords the records to crawl
    * @return the record set for the instanceHealth
    */
  def doCrawl(elbRecords: Seq[Record])(implicit req: RequestId): Seq[Record] = {
    val futures: Seq[java.util.concurrent.Future[Record]] = elbRecords.map(
      elb => {
        threadPool.submit(
          new Callable[Record] {
            def call() = {
              try {
                val instances = backoffRequest { ctx.awsClient.elb.describeInstanceHealth(DescribeInstanceHealthRequest.builder().loadBalancerName(elb.id).build).instanceStates }
                elb.copy(data = Map("name" -> elb.id, "instances" -> instances.asScala.map(ctx.beanMapper(_))))
              } catch {
                case e: Exception => {
                  throw new java.lang.RuntimeException(this + " describeInstanceHealth failed for ELB " + elb.id, e)
                }
              }
            }
          }
        )
      }
    )
    var failed: Boolean = false
    val records = futures.map(
      f => {
        try Some(f.get)
        catch {
          case e: Exception => {
            failed = true
            if (logger.isErrorEnabled) logger.error(s"$req$this exception from describeInstanceHealth", e)
            None
          }
        }
      }
    ).collect {
      case Some(rec) => rec
    }

    if (failed) {
      throw new java.lang.RuntimeException(s"$this failed to crawl instance health")
    }
    records
  }

  protected override def initState = addInitialState(super.initState, newLocalState(AwsInstanceHealthCrawlerState()))

  protected override def init() {
    implicit val req = RequestId("init")
    import Utils._
    Utils.namedActor(this + " init") {
      import ObserverExecutionContext._
      crawler.addObserver(this) onComplete {
        case scala.util.Failure(msg) => {
          if (logger.isErrorEnabled) logger.error(s"$req${Actor.self} failed to add observer $this to $crawler: $msg, retrying")
          this.init
        }
        case scala.util.Success(msg) => super.init
      }
    }
  }

  protected def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (gotMsg @ Crawler.CrawlResult(from, elbRecordSet), state) => {
      implicit val req = gotMsg.req
      // this is blocking so we dont crawl in parallel
      if (elbRecordSet.records ne localState(state).elbRecords) {
        val stopwatch = crawlTimer.start()
        val newRecords = doCrawl(elbRecordSet.records)
        stopwatch.stop()
        if (logger.isInfoEnabled) logger.info("{} {} Crawled {} records in {} sec", Utils.toObjects(
          req, this, newRecords.size, stopwatch.getDuration(TimeUnit.MILLISECONDS) / 1000D -> "%.2f"))
        Observable.localState(state).observers.foreach(_ ! Crawler.CrawlResult(this, RecordSet(newRecords, Map("source" -> "crawl", "req" -> req.id))))
        /* reset retry count at end of run for backoff */
        retry_count = 0
        setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), AwsInstanceHealthCrawlerState(elbRecordSet.records))
      } else state
    }
  }

  override protected def transitions = localTransitions orElse super.transitions
}


/** crawler for LaunchConfigurations
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsLaunchConfigurationCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = DescribeLaunchConfigurationsRequest.builder().maxRecords(50).build()

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator() {
      def next() = {
        val response = backoffRequest { ctx.awsClient.asg.describeLaunchConfigurations(request.toBuilder().nextToken(this.nextToken.get).build()) }
        this.nextToken = Option(response.nextToken)
        response.launchConfigurations.asScala.map(
          item => Record(item.launchConfigurationName, item.createdTime, ctx.beanMapper(item))).toList
      }
    }
    it.toList.flatten
  }
}

/** crawler for Network Interfaces
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsNetworkInterfaceCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = DescribeNetworkInterfacesRequest.builder().build()

  override def doCrawl()(implicit req: RequestId) = {
    val list = backoffRequest { ctx.awsClient.ec2.describeNetworkInterfaces(request).networkInterfaces }.asScala.map(
      item => {
        Record(item.networkInterfaceId, ctx.beanMapper(item))
      }
    )
    list
  }
}

/** crawler for Reservations (ie group of instances, not pre-paid reserved instances)
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsReservationCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = DescribeInstancesRequest.builder().build()

  lazy val abortWithoutTags = Utils.getProperty("edda.crawler", "abortWithoutTags", name, "false")
  override def doCrawl()(implicit req: RequestId) = {
    var tagCount = 0
    val list = backoffRequest { ctx.awsClient.ec2.describeInstances(request).reservations }.asScala.map(
      item => {
        tagCount += item.instances.asScala.map(_.tags.size).sum
        Record(item.reservationId, ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && abortWithoutTags.get.toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

case class AwsInstanceCrawlerState(reservationRecords: Seq[Record] = Seq[Record]())

object AwsInstanceCrawler extends StateMachine.LocalState[AwsInstanceCrawlerState]

/** crawler for Instances
  *
  * this is a secondary crawler that takes the results from the AwsReservationCrawler
  * and then pulls out each instance in the reservations to track them seperately
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  * @param crawler the AwsReservation crawler
  */
class AwsInstanceCrawler(val name: String, val ctx: AwsCrawler.Context, val crawler: Crawler) extends Crawler {

  import AwsInstanceCrawler._

  private[this] val logger = LoggerFactory.getLogger(getClass)

  override def crawl()(implicit req: RequestId) {}

  // we dont crawl, just get updates from crawler when it crawls
  override def doCrawl()(implicit req: RequestId) = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on InstanceCrawler")

  def doCrawl(resRecords: Seq[Record]): Seq[Record] = {
    resRecords.flatMap(rec => {
      rec.data.asInstanceOf[Map[String, Any]].get("instances") match {
        case Some(instances: Seq[_]) => instances.asInstanceOf[Seq[Map[String,Any]]].map(
          (inst: Map[String, Any]) => rec.copy(
            id = inst("instanceId").asInstanceOf[String],
            data = inst,
            ctime = inst("launchTime").asInstanceOf[Instant]))
        case other => throw new java.lang.RuntimeException("failed to crawl instances from reservation, got: " + other)
      }
    })
  }

  protected override def initState = addInitialState(super.initState, newLocalState(AwsInstanceCrawlerState()))

  protected override def init() {
    implicit val req = RequestId("init")
    import Utils._
    Utils.namedActor(this + " init") {
      import ObserverExecutionContext._
      crawler.addObserver(this) onComplete {
        case scala.util.Failure(msg) => {
          if (logger.isErrorEnabled) logger.error(s"$req{Actor.self} failed to add observer $this $crawler: $msg, retrying")
          this.init
        }
        case scala.util.Success(msg) => super.init
      }
    }
  }

  protected def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (gotMsg @ Crawler.CrawlResult(from, reservationSet), state) => {
      implicit val req = gotMsg.req
      // this is blocking so we dont crawl in parallel
      if (reservationSet.records ne localState(state).reservationRecords) {
        val newRecords = doCrawl(reservationSet.records)
        Observable.localState(state).observers.foreach(_ ! Crawler.CrawlResult(this, RecordSet(newRecords, Map("source" -> "crawl", "req" -> req.id))))
        setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), AwsInstanceCrawlerState(reservationSet.records))
      } else state
    }
  }

  override protected def transitions = localTransitions orElse super.transitions
}

/** crawler for Security Group
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsSecurityGroupCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = DescribeSecurityGroupsRequest.builder().build()
  lazy val abortWithoutTags = Utils.getProperty("edda.crawler", "abortWithoutTags", name, "false")

  override def doCrawl()(implicit req: RequestId) = {
    var tagCount = 0
    val list = backoffRequest { ctx.awsClient.ec2.describeSecurityGroups(request).securityGroups }.asScala.map(
      item => {
        tagCount += item.tags.size
        Record(item.groupId, ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && abortWithoutTags.get.toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

/** crawler for Snapshots
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsSnapshotCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = DescribeSnapshotsRequest.builder().build()
  lazy val abortWithoutTags = Utils.getProperty("edda.crawler", "abortWithoutTags", name, "false")

  override def doCrawl()(implicit req: RequestId) = {
    var tagCount = 0
    val list = backoffRequest { ctx.awsClient.ec2.describeSnapshots(request).snapshots }.asScala.map(
      item => {
        tagCount += item.tags.size
        Record(item.snapshotId, item.startTime, ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && abortWithoutTags.get.toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

/** crawler for all Tags
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsTagCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = DescribeTagsRequest.builder().build()

  override def doCrawl()(implicit req: RequestId) = backoffRequest { ctx.awsClient.ec2.describeTags(request).tags }.asScala.map(
    item => Record(item.key() + "|" + item.resourceType + "|" + item.resourceId, ctx.beanMapper(item))).toSeq
}

/** crawler for Volumes
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsVolumeCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = DescribeVolumesRequest.builder().build()
  lazy val abortWithoutTags = Utils.getProperty("edda.crawler", "abortWithoutTags", name, "false")

  override def doCrawl()(implicit req: RequestId) = {
    var tagCount = 0
    val list = backoffRequest { ctx.awsClient.ec2.describeVolumes(request).volumes }.asScala.map(
      item => {
        tagCount += item.tags.size
        Record(item.volumeId, item.createTime, ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && abortWithoutTags.get.toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

/** crawler for S3 Buckets
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsBucketCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = ListBucketsRequest.builder().build()

  override def doCrawl()(implicit req: RequestId) = backoffRequest { ctx.awsClient.s3.listBuckets(request).buckets }.asScala.map(
    item => Record(item.name, item.creationDate, ctx.beanMapper(item))).toSeq
}

/** crawler for IAM Users
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsIamUserCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = ListUsersRequest.builder().build()
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(10)

  override def doCrawl()(implicit req: RequestId) = {
    val users = backoffRequest { ctx.awsClient.identitymanagement.listUsers(request).users.asScala }
    val futures: Seq[java.util.concurrent.Future[Record]] = users.map(
      user => {
        threadPool.submit(
          new Callable[Record] {
            def call() = {
              val groupsRequest = ListGroupsForUserRequest.builder().userName(user.userName).build()
              val groups = backoffRequest { ctx.awsClient.identitymanagement.listGroupsForUser(groupsRequest).groups }.asScala.map( item => item.groupName ).toSeq
              val accessKeysRequest = ListAccessKeysRequest.builder().userName(user.userName).build()
              val accessKeys = Map[String, String]() ++ backoffRequest { ctx.awsClient.identitymanagement.listAccessKeys(accessKeysRequest).accessKeyMetadata }.asScala.map(item => ctx.beanMapper(item)).toSeq
              val userPoliciesRequest = ListUserPoliciesRequest.builder().userName(user.userName).build()
              val userPolicies = backoffRequest { ctx.awsClient.identitymanagement.listUserPolicies(userPoliciesRequest).policyNames.asScala }
              Record(user.userName, user.createDate, Map("name" -> user.userName, "attributes" -> (ctx.beanMapper(user)), "groups" -> groups, "accessKeys" -> accessKeys, "userPolicies" -> userPolicies))
            }
          }
        )
      }
    )
    var failed = false
    val records = futures.map(
      f => {
        try Some(f.get)
        catch {
          case e: Exception => {
            if (logger.isErrorEnabled) logger.error(s"$req$this exception from IAM user sub requests", e)
            failed = true
            None
          }
        }
      }
    ).collect {
      case Some(rec) => rec
    }

    if (failed) {
      throw new java.lang.RuntimeException(s"$this failed to crawl resource record sets")
    }
    records
  }

}

/** crawler for IAM Groups
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsIamGroupCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = ListGroupsRequest.builder().build()
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(10)

  override def doCrawl()(implicit req: RequestId) = {
    val groups = backoffRequest { ctx.awsClient.identitymanagement.listGroups(request).groups.asScala }
    val futures: Seq[java.util.concurrent.Future[Record]] = groups.map(
      group => {
        threadPool.submit(
          new Callable[Record] {
            def call() = {
              val groupPoliciesRequest = ListGroupPoliciesRequest.builder().groupName(group.groupName).build()
              val groupPolicies = backoffRequest { ctx.awsClient.identitymanagement.listGroupPolicies(groupPoliciesRequest).policyNames.asScala.toSeq }
              Record(group.groupName, group.createDate, Map("name" -> group.groupName, "attributes" -> (ctx.beanMapper(group)), "policies" -> groupPolicies))
            }
          }
        )
      }
    )
    var failed = false
    val records = futures.map(
      f => {
        try Some(f.get)
        catch {
          case e: Exception => {
            if (logger.isErrorEnabled) logger.error(s"$req$this exception from IAM listGroupPolicies", e)
            failed = true
            None
          }
        }
      }
    ).collect {
      case Some(rec) => rec
    }

    if (failed) {
      throw new java.lang.RuntimeException(s"$this failed to crawl resource record sets")
    }
    records
  }

}

/** crawler for IAM Roles
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsIamRoleCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = ListRolesRequest.builder().build()

  override def doCrawl()(implicit req: RequestId) = backoffRequest { ctx.awsClient.identitymanagement.listRoles(request).roles }.asScala.map(
    item => Record(item.roleName, item.createDate, ctx.beanMapper(item))).toSeq
}

/** crawler for IAM policies
  *
  * @param name name of context we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsIamPolicyCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = ListPoliciesRequest.builder().build()

  override def doCrawl()(implicit req: RequestId) = {
    ctx.awsClient.identitymanagement.listPolicies(request).policies.asScala.map(
      item => {
        val vr = GetPolicyVersionRequest.builder()
          .policyArn(item.arn)
          .versionId(item.defaultVersionId)
          .build()
        val version = ctx.awsClient.identitymanagement.getPolicyVersion(vr).policyVersion

        val data = ctx.beanMapper(item).asInstanceOf[Map[String, Any]] ++ Map("defaultDocument" -> version.document)

        Record(item.policyName, item.updateDate, data)
      }).toSeq
  }
}


/** crawler for IAM Policy Versions
  *
  */
class AwsIamPolicyVersionCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = ListPolicyVersionsRequest.builder().build()

  override def doCrawl()(implicit req: RequestId) = ctx.awsClient.identitymanagement.listPolicyVersions(request).versions.asScala.map(
    item => Record(item.versionId, item.createDate, ctx.beanMapper(item))).toSeq
}

/** crawler for IAM Virtual MFA Devices
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper and configuration
  */
class AwsIamVirtualMFADeviceCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = ListVirtualMFADevicesRequest.builder().build()

  override def doCrawl()(implicit req: RequestId) = backoffRequest { ctx.awsClient.identitymanagement.listVirtualMFADevices(request).virtualMFADevices }.asScala.map(
    item => Record(item.serialNumber.split('/').last, item.enableDate, ctx.beanMapper(item))).toSeq
}

/** crawler for SQS Queues
  *
  * This crawler is similar to the InstanceHealth crawler in that it has to first
  * get a list of SQS Queues then for each queue fan-out and query each queue.
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsSimpleQueueCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = ListQueuesRequest.builder().build()
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(10)

  override def doCrawl()(implicit req: RequestId) = {
    val queues = backoffRequest { ctx.awsClient.sqs.listQueues(request).queueUrls.asScala }
    val futures: Seq[java.util.concurrent.Future[Record]] = queues.map(
      queueUrl => {
        threadPool.submit(
          new Callable[Record] {
            def call() = {
              val name = queueUrl.split('/').last
              val attrRequest = GetQueueAttributesRequest.builder().queueUrl(queueUrl).attributeNames("All").build()
              val attrs = Map[String, String]() ++ backoffRequest { ctx.awsClient.sqs.getQueueAttributes(attrRequest).attributes.asScala }
              val ctime = attrs.get("CreatedTimestamp") match {
                case Some(time) => Instant.ofEpochSecond(time.toInt)
                case None => Instant.now
              }

              Record(name, ctime, Map("name" -> name, "url" -> queueUrl, "attributes" -> (attrs)))
            }
          }
        )
      }
    )
    var failed: Boolean = false
    val records = futures.map(
      f => {
        try Some(f.get)
        catch {
          case e: java.util.concurrent.ExecutionException => {
            e.getCause match {
              case e: SdkServiceException if e.errorCode == "AWS.SimpleQueueService.NonExistentQueue" => {
                // this happens constantly, dont log it.  There is a large time delta between queues being deleted
                // but still showing up in the ListQueuesResult
                None
              }
              case e: Throwable => {
                if (logger.isErrorEnabled) logger.error(s"$req$this exception from SQS getQueueAttributes", e)
                failed = true
                None
              }
            }
          }
          case e: Throwable => {
            if (logger.isErrorEnabled) logger.error(s"$req$this exception from SQS getQueueAttributes", e)
            failed = true
            None
          }
        }
      }
    ).collect {
      case Some(rec) => rec
    }

    if (failed) {
      throw new java.lang.RuntimeException(s"$this failed to crawl resource record sets")
    }
    records
  }
}

/** crawler for ReservedInstance (ie pre-paid instances)
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsReservedInstanceCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = DescribeReservedInstancesRequest.builder().build()

  override def doCrawl()(implicit req: RequestId) = backoffRequest { ctx.awsClient.ec2.describeReservedInstances(request).reservedInstances }.asScala.map(
    item => Record(item.reservedInstancesId, item.start, ctx.beanMapper(item))).toSeq
}

/** crawler for ReservedInstancesOfferings
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsReservedInstancesOfferingCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = DescribeReservedInstancesOfferingsRequest.builder().build()

  override def doCrawl()(implicit req: RequestId) = backoffRequest { ctx.awsClient.ec2.describeReservedInstancesOfferings(request).reservedInstancesOfferings }.asScala.map(
    item => Record(item.reservedInstancesOfferingId, ctx.beanMapper(item))).toSeq
}

/** crawler for Route53 Hosted Zones (DNS records)
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsHostedZoneCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = ListHostedZonesRequest.builder().build()

  override def doCrawl()(implicit req: RequestId) =  backoffRequest { ctx.awsClient.route53.listHostedZones(request).hostedZones }.asScala.map(
      item => Record(item.name, ctx.beanMapper(item))).toSeq
}

case class AwsHostedRecordCrawlerState(hostedZones: Seq[Record] = Seq[Record]())

object AwsHostedRecordCrawler extends StateMachine.LocalState[AwsHostedRecordCrawlerState]

/** crawler for Route53 Resource Record Sets (DNS records)
  *  this is a secondary crawler that crawls the resource recordsets for each hosted zone
  * and then pulls out each recordset in the zones to track them seperately
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  * @param crawler the awsHostedZone crawler
  */
class AwsHostedRecordCrawler(val name: String, val ctx: AwsCrawler.Context, val crawler: Crawler) extends Crawler {

  import AwsHostedRecordCrawler._

  override def crawl()(implicit req: RequestId) {}

  // we dont crawl, just get updates from crawler when it crawls
  override def doCrawl()(implicit req: RequestId) = throw new java.lang.UnsupportedOperationException("doCrawl() should not be called on HostedRecordCrawler")

  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(10)
  /** for each zone call listResourceRecordSets and map that to a new document
    *
    * @param zones the records to crawl
    * @return the record set for the resourceRecordSet
    */
  def doCrawl(zones: Seq[Record])(implicit req: RequestId): Seq[Record] = {

    val futures: Seq[java.util.concurrent.Future[Seq[Record]]] = zones.map(
      zone => {
        val zoneId = zone.data.asInstanceOf[Map[String,Any]]("id").asInstanceOf[String]
        val zoneName = zone.id
        val request = ListResourceRecordSetsRequest.builder().hostedZoneId(zoneId).build()
        threadPool.submit(
          new Callable[Seq[Record]] {
            def call() = {
              val it = new AwsIterator() {
                def next() = {
                  val response = backoffRequest { ctx.awsClient.route53.listResourceRecordSets(request.toBuilder().startRecordName(this.nextToken.get).build()) }
                  this.nextToken = Option(response.nextRecordName)
                  response.resourceRecordSets.asScala.map(
                    item => {
                      Record(item.name, ctx.beanMapper(item).asInstanceOf[Map[String,Any]] ++ Map("zone" -> Map("id" -> zoneId, "name" -> zoneName)))
                    }
                  ).toList
                }
              }
              it.toList.flatten
            }
          }
        )
      }
    )
    var failed: Boolean = false
    val records = futures.map(
      f => {
        try Some(f.get)
        catch {
          case e: Exception => {
            failed = true
            if (logger.isErrorEnabled) logger.error(s"$req$this exception from listResourceRecordSets", e)
            None
          }
        }
      }
    ).collect({
      case Some(rec) => rec
    }).flatten

    if (failed) {
      throw new java.lang.RuntimeException(s"$this failed to crawl resource record sets")
    }
    records
  }

  protected override def initState = addInitialState(super.initState, newLocalState(AwsHostedRecordCrawlerState()))

  protected override def init() {
    implicit val req = RequestId("init")
    import Utils._
    Utils.namedActor(this + " init") {
      import ObserverExecutionContext._
      crawler.addObserver(this) onComplete {
        case scala.util.Failure(msg) => {
          if (logger.isErrorEnabled) logger.error(s"$req${Actor.self} failed to add observer $this to $crawler: $msg, retrying")
          this.init
        }
        case scala.util.Success(msg) => super.init
      }
    }
  }

  protected def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (gotMsg @ Crawler.CrawlResult(from, hostedZoneSet), state) => {
      implicit val req = gotMsg.req
      // this is blocking so we dont crawl in parallel
      if (hostedZoneSet.records ne localState(state).hostedZones) {
        val newRecords = doCrawl(hostedZoneSet.records)
        Observable.localState(state).observers.foreach(_ ! Crawler.CrawlResult(this, RecordSet(newRecords, Map("source" -> "crawl", "req" -> req.id))))
        setLocalState(Crawler.setLocalState(state, CrawlerState(newRecords)), AwsHostedRecordCrawlerState(hostedZoneSet.records))
      } else state
    }
  }

  override protected def transitions = localTransitions orElse super.transitions
}

/** crawler for RDS Databases
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsDatabaseCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  val request = DescribeDBInstancesRequest.builder()
    .maxRecords(50)
    .build()

  override def doCrawl()(implicit req: RequestId) = {
    val it = new AwsIterator() {
      def next() = {
        // annoying, describeDBInstances has withMarker and getMarker instead if withToken and getNextToken
        val response = backoffRequest { ctx.awsClient.rds.describeDBInstances(request.toBuilder().marker(this.nextToken.get).build()) }
        this.nextToken = Option(response.marker)
        response.dbInstances.asScala.map(
          item => {
            Record(item.dbInstanceIdentifier, ctx.beanMapper(item))
          }).toList
      }
    }

    backoffRequest { ctx.awsClient.loadAccountNum() }

    val initial = it.toList.flatten
    var buffer = new ListBuffer[Record]()
    for (rec <- initial) {
        val data = rec.toMap("data").asInstanceOf[Map[String,String]]
        val arn = ctx.awsClient.arn("rds", "db", data("DBInstanceIdentifier"))

        val request = ListTagsForResourceRequest.builder().resourceName(arn).build()
        val response = backoffRequest { ctx.awsClient.rds.listTagsForResource(request) }
        val responseList = response.tagList.asScala.map(
          item => {
            ctx.beanMapper(item)
          }).toList

        buffer += rec.copy(data = data.asInstanceOf[Map[String,Any]] ++ Map("arn" -> arn, "tags" -> responseList))
    }
    buffer.toList
  }
}

/** crawler for Subnets
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsDatabaseSubnetCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  override def doCrawl()(implicit req: RequestId) =
    ctx.awsClient.rds.describeDBSubnetGroups().dbSubnetGroups.asScala.map(
      item => {
        Record(item.dbSubnetGroupName, ctx.beanMapper(item))
      })
}

/** crawler for ElastiCache Clusters
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsCacheClusterCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = DescribeCacheClustersRequest.builder().build()

  override def doCrawl()(implicit req: RequestId) = backoffRequest { ctx.awsClient.elasticache.describeCacheClusters(request).cacheClusters }.asScala.map(
    item => Record(item.cacheClusterId, ctx.beanMapper(item))).toSeq
}

/** crawler for Subnets
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsSubnetCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  val request = DescribeSubnetsRequest.builder().build()
  lazy val abortWithoutTags = Utils.getProperty("edda.crawler", "abortWithoutTags", name, "false")

  override def doCrawl()(implicit req: RequestId) = {
    var tagCount = 0
    val list = backoffRequest { ctx.awsClient.ec2.describeSubnets(request).subnets }.asScala.map(
      item => {
        tagCount += item.tags.size
        Record(item.subnetId, ctx.beanMapper(item))
      }).toSeq
    if (tagCount == 0 && abortWithoutTags.get.toBoolean) {
      throw new java.lang.RuntimeException("no tags found for " + name + ", ignoring crawl results")
    }
    list
  }
}

/** crawler for Cloudformation Stacks
  *
  * @param name name of collection we are crawling for
  * @param ctx context to provide beanMapper
  */
class AwsCloudformationCrawler(val name: String, val ctx: AwsCrawler.Context) extends Crawler {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = Executors.newFixedThreadPool(1)

  private def getStacksFromAws: List[Stack] = {
    val stacks = List.newBuilder[Stack]
    val request = DescribeStacksRequest.builder().build()
    var token: String = null
    do {
      val response = backoffRequest {
        ctx.awsClient.cloudformation.describeStacks(request.toBuilder().nextToken(token).build())
      }
      stacks ++= response.stacks.asScala
      token = response.nextToken
    } while (token != null)
    stacks.result
  }

  override def doCrawl()(implicit req: RequestId) = {
    val stacks = getStacksFromAws
    val futures: Seq[java.util.concurrent.Future[Record]] = stacks.map(
      stack => {
        this.threadPool.submit(
          new Callable[Record] {
            def call() = {
              val stackResourcesRequest = ListStackResourcesRequest.builder().stackName(stack.stackName).build()
              val stackResources = backoffRequest { ctx.awsClient.cloudformation.listStackResources(stackResourcesRequest).stackResourceSummaries.asScala.map(item => ctx.beanMapper(item)) }
              Record(stack.stackName, stack.creationTime, ctx.beanMapper(stack).asInstanceOf[Map[String,Any]] ++ Map("resources" -> stackResources))
            }
          }
        )
      }
    )
    val records = futures.map(
      f => {
        try Some(f.get)
        catch {
          case e: Exception => {
            if (logger.isErrorEnabled) logger.error(this + "exception from Cloudformation listStackResources", e)
            None
          }
        }
      }
    ).collect {
      case Some(rec) => rec
    }
    records
  }

}
