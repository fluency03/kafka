/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.controller

import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.common.{LeaderElectionNotNeededException, NoReplicaOnlineException, StateChangeFailedException, TopicAndPartition}
import kafka.log.LogConfig
import kafka.server.{ConfigType, KafkaConfig}
import kafka.utils.Logging

trait PartitionLeaderSelector {

  /**
   * @param topicAndPartition          The topic and partition whose leader needs to be elected
   * @param currentLeaderAndIsr        The current leader and isr of input partition read from zookeeper
   * @throws NoReplicaOnlineException If no replica in the assigned replicas list is alive
   * @return The leader and isr request, with the newly selected leader and isr, and the set of replicas to receive
   * the LeaderAndIsrRequest.
   */
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int])

}

/**
 * Select the new leader, new isr and receiving replicas (for the LeaderAndIsrRequest):
 * 1. If at least one broker from the isr is alive, it picks a broker from the live isr as the new leader and the live
 *    isr as the new isr.
 * 2. Else, if unclean leader election for the topic is disabled, it throws a NoReplicaOnlineException.
 * 3. Else, it picks some alive broker from the assigned replica list as the new leader and the new isr.
 * 4. If no broker in the assigned replica list is alive, it throws a NoReplicaOnlineException
 * Replicas to receive LeaderAndIsr request = live assigned replicas
 * Once the leader is successfully registered in zookeeper, it updates the allLeaders cache
 */
class OfflinePartitionLeaderSelector(controllerContext: ControllerContext, config: KafkaConfig)
  extends PartitionLeaderSelector with Logging {

  logIdent = "[OfflinePartitionLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    // fluency03: get assigned replicas based on the topicAndPartition
    controllerContext.partitionReplicaAssignment.get(topicAndPartition) match {
      case Some(assignedReplicas) =>
        // fluency03: get live (online) assigned replicas
        val liveAssignedReplicas = assignedReplicas.filter(r => controllerContext.isReplicaOnline(r, topicAndPartition))
        // fluency03: get live (online) brokers in ISR
        val liveBrokersInIsr = currentLeaderAndIsr.isr.filter(r => controllerContext.isReplicaOnline(r, topicAndPartition))
        val newLeaderAndIsr =
          // fluency03: no live (online) brokers in ISR
          if (liveBrokersInIsr.isEmpty) {
            // Prior to electing an unclean (i.e. non-ISR) leader, ensure that doing so is not disallowed by the configuration
            // for unclean leader election.
            // fluency03: no live (online) brokers in ISR, we have to choose leader from the non-ISR replicas
            // fluency03: if UncleanLeaderElection is not enabled, throw an NoReplicaOnlineException
            if (!LogConfig.fromProps(config.originals, AdminUtils.fetchEntityConfig(controllerContext.zkUtils,
              ConfigType.Topic, topicAndPartition.topic)).uncleanLeaderElectionEnable) {
              throw new NoReplicaOnlineException(
                s"No replica in ISR for partition $topicAndPartition is alive. Live brokers are: [${controllerContext.liveBrokerIds}], " +
                  s"ISR brokers are: [${currentLeaderAndIsr.isr.mkString(",")}]"
              )
            }
            debug(s"No broker in ISR is alive for $topicAndPartition. Pick the leader from the alive assigned " +
              s"replicas: ${liveAssignedReplicas.mkString(",")}")

            // fluency03: UncleanLeaderElection is enabled
            // fluency03: if there is no non-ISR live replicas, throw an NoReplicaOnlineException
            if (liveAssignedReplicas.isEmpty) {
              throw new NoReplicaOnlineException(s"No replica for partition $topicAndPartition is alive. Live " +
                s"brokers are: [${controllerContext.liveBrokerIds}]. Assigned replicas are: [$assignedReplicas].")
            } else {
              controllerContext.stats.uncleanLeaderElectionRate.mark()
              // fluency03: get the first live replica
              val newLeader = liveAssignedReplicas.head
              warn(s"No broker in ISR is alive for $topicAndPartition. Elect leader $newLeader from live " +
                s"brokers ${liveAssignedReplicas.mkString(",")}. There's potential data loss.")
              // fluency03: set the first replica as the leaders
              currentLeaderAndIsr.newLeaderAndIsr(newLeader, List(newLeader))
            }
          // fluency03: there are existing live (online) brokers in ISR
          } else {
            // fluency03: get live replicas in ISR
            val liveReplicasInIsr = liveAssignedReplicas.filter(r => liveBrokersInIsr.contains(r))
            // fluency03: obtain the first replica
            val newLeader = liveReplicasInIsr.head
            debug(s"Some broker in ISR is alive for $topicAndPartition. Select $newLeader from ISR " +
              s"${liveBrokersInIsr.mkString(",")} to be the leader.")
            // fluency03: set the first replica as the leader
            currentLeaderAndIsr.newLeaderAndIsr(newLeader, liveBrokersInIsr)
          }
        info(s"Selected new leader and ISR $newLeaderAndIsr for offline partition $topicAndPartition")
        (newLeaderAndIsr, liveAssignedReplicas)
      // fluency03: no replicas found for given topicAndPartition
      case None =>
        throw new NoReplicaOnlineException(s"Partition $topicAndPartition doesn't have replicas assigned to it")
    }
  }
}

/**
 * fluency03: New leader = a live in-sync reassigned replica
 * fluency03: New isr = current isr
 * fluency03: Replicas to receive LeaderAndIsr request = reassigned replicas
 */
// fluency03: leader selection after partition reassignment
class ReassignedPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  logIdent = "[ReassignedPartitionLeaderSelector]: "

  /**
   * The reassigned replicas are already in the ISR when selectLeader is called.
   */
  def selectLeader(topicAndPartition: TopicAndPartition,
                   currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    // fluency03: get the new replicas from reassigned partition based on specified stopicAndPartition
    val reassignedInSyncReplicas = controllerContext.partitionsBeingReassigned(topicAndPartition).newReplicas
    // fluency03: find one replica (Option), which is live (online) and part of ISR
    val newLeaderOpt = reassignedInSyncReplicas.find { r =>
      controllerContext.isReplicaOnline(r, topicAndPartition) && currentLeaderAndIsr.isr.contains(r)
    }
    newLeaderOpt match {
      // fluency03: set the new leader and reassigned ISR replicas
      case Some(newLeader) => (currentLeaderAndIsr.newLeader(newLeader), reassignedInSyncReplicas)
      // fluency03: no required new leader found, throw an NoReplicaOnlineException
      case None =>
        val errorMessage = if (reassignedInSyncReplicas.isEmpty) {
          s"List of reassigned replicas for partition $topicAndPartition is empty. Current leader and ISR: " +
            s"[$currentLeaderAndIsr]"
        } else {
          s"None of the reassigned replicas for partition $topicAndPartition are in-sync with the leader. " +
            s"Current leader and ISR: [$currentLeaderAndIsr]"
        }
        throw new NoReplicaOnlineException(errorMessage)
    }
  }
}

/**
 * fluency03: New leader = preferred (first assigned) replica (if in isr and alive);
 * fluency03: New isr = current isr;
 * fluency03: Replicas to receive LeaderAndIsr request = assigned replicas
 */
// fluency03: set the leader as the first assigned replica
class PreferredReplicaPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  logIdent = "[PreferredReplicaPartitionLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition,
                   currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    val preferredReplica = assignedReplicas.head
    // check if preferred replica is the current leader
    val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
    if (currentLeader == preferredReplica) {
      throw new LeaderElectionNotNeededException("Preferred replica %d is already the current leader for partition %s"
                                                   .format(preferredReplica, topicAndPartition))
    } else {
      info("Current leader %d for partition %s is not the preferred replica.".format(currentLeader, topicAndPartition) +
        " Triggering preferred replica leader election")
      // check if preferred replica is not the current leader and is alive and in the isr
      if (controllerContext.isReplicaOnline(preferredReplica, topicAndPartition) && currentLeaderAndIsr.isr.contains(preferredReplica)) {
        val newLeaderAndIsr = currentLeaderAndIsr.newLeader(preferredReplica)
        (newLeaderAndIsr, assignedReplicas)
      } else {
        throw new StateChangeFailedException(s"Preferred replica $preferredReplica for partition $topicAndPartition " +
          s"is either not alive or not in the isr. Current leader and ISR: [$currentLeaderAndIsr]")
      }
    }
  }
}

/**
 * fluency03: New leader = replica in isr that's not being shutdown;
 * fluency03: New isr = current isr - shutdown replica;
 * fluency03: Replicas to receive LeaderAndIsr request = live assigned replicas
 */
// fluency03: leader election during ControlledShutdown
class ControlledShutdownLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  logIdent = "[ControlledShutdownLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition,
                   currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    val currentIsr = currentLeaderAndIsr.isr
    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    val liveAssignedReplicas = assignedReplicas.filter(r => controllerContext.isReplicaOnline(r, topicAndPartition, true))

    // fluency03: get new ISR replicas from current ISR, by removing the shutting down replica
    val newIsr = currentIsr.filter(brokerId => !controllerContext.shuttingDownBrokerIds.contains(brokerId))
    liveAssignedReplicas.find(newIsr.contains) match {
      case Some(newLeader) =>
        debug(s"Partition $topicAndPartition : current leader = ${currentLeaderAndIsr.leader}, new leader = $newLeader")
        val newLeaderAndIsr = currentLeaderAndIsr.newLeaderAndIsr(newLeader, newIsr)
        (newLeaderAndIsr, liveAssignedReplicas)
      case None =>
        throw new StateChangeFailedException(s"No other replicas in ISR ${currentIsr.mkString(",")} for $topicAndPartition " +
          s"besides shutting down brokers ${controllerContext.shuttingDownBrokerIds.mkString(",")}")
    }
  }
}

/**
 * fluency03: Essentially does nothing. Returns the current leader and ISR, and the current
 * fluency03: set of replicas assigned to a given topic/partition.
 */
class NoOpLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  logIdent = "[NoOpLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition,
                   currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    warn("I should never have been asked to perform leader election, returning the current LeaderAndIsr and replica assignment.")
    (currentLeaderAndIsr, controllerContext.partitionReplicaAssignment(topicAndPartition))
  }
}
