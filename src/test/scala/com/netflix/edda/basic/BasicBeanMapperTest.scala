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
package com.netflix.edda.basic

import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import java.util.{LinkedList => JList}
import java.util.Date
import java.time.Instant
import software.amazon.awssdk.services.autoscaling.model.AutoScalingGroup
import software.amazon.awssdk.services.autoscaling.model.Instance
import software.amazon.awssdk.services.autoscaling.model.TagDescription

import org.scalatest.FunSuite

class BasicBeanMapperTest extends FunSuite {
  val logger = LoggerFactory.getLogger(getClass)
  test("fromBean") {
    val mapper = new BasicBeanMapper

    val azs = new JList[String]()
    azs.add("us-east-1c")

    val inst1 = Instance.builder()
      .availabilityZone("us-east-1c")
      .healthStatus("Healthy")
      .instanceId("i-0123456789")
      .launchConfigurationName("launchConfigName")
      .lifecycleState("InService")
      .build()
    val instances = new JList[Instance]()
    instances.add(inst1)

    val elbs = new JList[String]()
    elbs.add("elbName")

    val tag = TagDescription.builder()
      .key("tagName")
      .value("tagValue")
      .build()
    val tags = new JList[TagDescription]()
    tags.add(tag)

    val asg = AutoScalingGroup.builder()
      .vpcZoneIdentifier("")
      .autoScalingGroupARN("ARN")
      .autoScalingGroupName("asgName")
      .availabilityZones(azs)
      .createdTime(Instant.ofEpochMilli(new Date(0).getTime()))
      .defaultCooldown(10)
      .desiredCapacity(2)
      .healthCheckGracePeriod(600)
      .healthCheckType("EC2")
      .instances(instances)
      .launchConfigurationName("launchConfigName")
      .loadBalancerNames(elbs)
      .maxSize(2)
      .minSize(2)
      .tags(tags)
      .build()
    val expected = 


Map(
    "terminationPolicies" -> null,
    "healthCheckGracePeriod" -> 600,
     "tags" -> List(
         Map(
            "resourceId" -> null,
            "resourceType" -> null, 
            "key" -> "tagName", 
            "class" -> "software.amazon.awssdk.services.autoscaling.model.TagDescription", 
            "propagateAtLaunch" -> null, 
            "value" -> "tagValue"
            )
        ),
    "healthCheckType" -> "EC2",
    "autoScalingGroupARN" -> "ARN",
    "placementGroup" -> null,
    "instances" -> List(
        Map(
            "instanceId" -> "i-0123456789",
            "healthStatus" -> "Healthy",
            "availabilityZone" -> "us-east-1c",
            "protectedFromScaleIn" -> null,
            "lifecycleState" -> Map(
                "class" -> "software.amazon.awssdk.services.autoscaling.model.LifecycleState",
                "name" -> "IN_SERVICE"
            ), 
            "class" -> "software.amazon.awssdk.services.autoscaling.model.Instance",
            "lifecycleStateString" -> "InService",
            "launchConfigurationName" -> "launchConfigName"
            )
        ),
    "vpcZoneIdentifier" -> "",
    "defaultCooldown" -> 10,
    "loadBalancerNames" -> List("elbName"), 
    "createdTime" -> Instant.ofEpochMilli(0),
    "suspendedProcesses" -> null,
    "status" -> null,
    "desiredCapacity" -> 2,
    "class" -> "software.amazon.awssdk.services.autoscaling.model.AutoScalingGroup",
    "enabledMetrics" -> null,
    "newInstancesProtectedFromScaleIn" -> null,
    "maxSize" -> 2,
    "targetGroupARNs" -> null,
    "availabilityZones" -> List("us-east-1c"),
    "autoScalingGroupName" -> "asgName",
    "minSize" -> 2,
    "launchConfigurationName" -> "launchConfigName"
    )

    expectResult(expected) {
      mapper.fromBean(asg)
    }

    val pf1: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
      case (obj: software.amazon.awssdk.services.autoscaling.model.TagDescription, "value", Some(x: Any)) if obj.key == "tagName" => None
    }
    mapper.addKeyMapper(pf1)

    expectResult(expected + ("tags" -> List(expected("tags").asInstanceOf[List[Map[String, Any]]].head - "value"))) {
      mapper.fromBean(asg)
    }

    val pf2: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
      case (obj: software.amazon.awssdk.services.autoscaling.model.TagDescription, "value", Some(x: Any)) if obj.key == "tagName" => Some("newValue")
    }
    mapper.addKeyMapper(pf2)

    expectResult(expected + ("tags" -> List(expected("tags").asInstanceOf[List[Map[String, Any]]].head + ("value" -> "newValue")))) {
      mapper.fromBean(asg)
    }

    // reset keyMapper
    val pf3: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
      case (obj, key, opt) => opt
    }
    mapper.addKeyMapper(pf3)

    // create object mapper to trim out "instances"
    val objMapper1: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
      case (obj, "instances", value) => None
    }
    mapper.addKeyMapper(objMapper1)

    expectResult(expected - "instances") {
      mapper.fromBean(asg)
    }

    // create object mapper to replace instances with empty list
    val objMapper2: PartialFunction[(AnyRef, String, Option[Any]), Option[Any]] = {
      case (obj, "instances", value) => Some(List[Instance]())
    }
    mapper.addKeyMapper(objMapper2)

    expectResult(expected + ("instances" -> List[Instance]())) {
      mapper.fromBean(asg)
    }
  }
}
