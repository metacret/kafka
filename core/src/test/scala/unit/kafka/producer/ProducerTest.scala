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

package kafka.producer

import org.scalatest.TestFailedException
import org.scalatest.junit.JUnit3Suite
import kafka.consumer.SimpleConsumer
import kafka.message.Message
import kafka.server.{KafkaConfig, KafkaRequestHandler, KafkaServer}
import kafka.zk.ZooKeeperTestHarness
import org.apache.log4j.{Level, Logger}
import org.junit.Test
import kafka.utils._
import java.util
import kafka.admin.CreateTopicCommand
import util.Properties
import kafka.api.FetchRequestBuilder
import kafka.common.FailedToSendMessageException
import org.junit.Assert.assertTrue
import org.junit.Assert.assertFalse
import org.junit.Assert.assertEquals



class ProducerTest extends JUnit3Suite with ZooKeeperTestHarness with Logging{
  private val brokerId1 = 0
  private val brokerId2 = 1
  private val ports = TestUtils.choosePorts(2)
  private val (port1, port2) = (ports(0), ports(1))
  private var server1: KafkaServer = null
  private var server2: KafkaServer = null
  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null
  private val requestHandlerLogger = Logger.getLogger(classOf[KafkaRequestHandler])
  private var servers = List.empty[KafkaServer]

  private val props1 = TestUtils.createBrokerConfig(brokerId1, port1)
  private val config1 = new KafkaConfig(props1) {
    override val hostName = "localhost"
    override val numPartitions = 4
  }
  private val props2 = TestUtils.createBrokerConfig(brokerId2, port2)
  private val config2 = new KafkaConfig(props2) {
    override val hostName = "localhost"
    override val numPartitions = 4
  }

  override def setUp() {
    super.setUp()
    // set up 2 brokers with 4 partitions each
    server1 = TestUtils.createServer(config1)
    server2 = TestUtils.createServer(config2)
    servers = List(server1,server2)

    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", port1.toString)

    consumer1 = new SimpleConsumer("localhost", port1, 1000000, 64*1024, "")
    consumer2 = new SimpleConsumer("localhost", port2, 100, 64*1024, "")

    // temporarily set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.FATAL)
  }

  override def tearDown() {
    // restore set request handler logger to a higher level
    requestHandlerLogger.setLevel(Level.ERROR)
    server1.shutdown
    server1.awaitShutdown()
    server2.shutdown
    server2.awaitShutdown()
    Utils.rm(server1.config.logDirs)
    Utils.rm(server2.config.logDirs)
    super.tearDown()
  }


  def testUpdateBrokerPartitionInfo() {
    val topic = "new-topic"
    CreateTopicCommand.createTopic(zkClient, "new-topic", 1, 2)
    // wait until the update metadata request for new topic reaches all servers
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0, 500)
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500)

    val props1 = new util.Properties()
    props1.put("metadata.broker.list", "localhost:80,localhost:81")
    props1.put("serializer.class", "kafka.serializer.StringEncoder")
    props1.put("producer.discovery", "static")
    val producerConfig1 = new ProducerConfig(props1)
    val producer1 = new Producer[String, String](producerConfig1)
    try{
      producer1.send(new KeyedMessage[String, String](topic, "test", "test1"))
      fail("Test should fail because the broker list provided are not valid")
    } catch {
      case e: FailedToSendMessageException =>
      case oe: Throwable => fail("fails with exception", oe)
    } finally {
      producer1.close()
    }

    val props2 = new util.Properties()
    props2.put("metadata.broker.list", "localhost:80," + TestUtils.getBrokerListStrFromConfigs(Seq( config1)))
    props2.put("producer.discovery", "static")
    props2.put("serializer.class", "kafka.serializer.StringEncoder")
    val producerConfig2= new ProducerConfig(props2)
    val producer2 = new Producer[String, String](producerConfig2)
    try{
      producer2.send(new KeyedMessage[String, String](topic, "test", "test1"))
    } catch {
      case e: Throwable => fail("Should succeed sending the message", e)
    } finally {
      producer2.close()
    }

    val props3 = new util.Properties()
    props3.put("metadata.broker.list", TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)))
    props3.put("producer.discovery", "static")
    props3.put("serializer.class", "kafka.serializer.StringEncoder")
    props3.put("producer.discovery", "static")
    val producerConfig3 = new ProducerConfig(props3)
    val producer3 = new Producer[String, String](producerConfig3)
    try{
      producer3.send(new KeyedMessage[String, String](topic, "test", "test1"))
    } catch {
      case e: Throwable => fail("Should succeed sending the message", e)
    } finally {
      producer3.close()
    }
  }

  @Test
  def testSendToNewTopic() {
    val props1 = new util.Properties()
    props1.put("serializer.class", "kafka.serializer.StringEncoder")
    props1.put("partitioner.class", "kafka.utils.StaticPartitioner")
    props1.put("metadata.broker.list", TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)))
    props1.put("producer.discovery", "static")
    props1.put("request.required.acks", "2")
    props1.put("request.timeout.ms", "1000")

    val props2 = new util.Properties()
    props2.putAll(props1)
    props2.put("request.required.acks", "3")
    props2.put("request.timeout.ms", "1000")
    props2.put("producer.discovery", "static")

    val producerConfig1 = new ProducerConfig(props1)
    val producerConfig2 = new ProducerConfig(props2)

    val topic = "new-topic"
    // create topic with 1 partition and await leadership
    CreateTopicCommand.createTopic(zkClient, topic, 1, 2)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0, 1000)
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500)

    val producer1 = new Producer[String, String](producerConfig1)
    val producer2 = new Producer[String, String](producerConfig2)
    // Available partition ids should be 0.
    producer1.send(new KeyedMessage[String, String](topic, "test", "test1"))
    producer1.send(new KeyedMessage[String, String](topic, "test", "test2"))
    // get the leader
    val leaderOpt = ZkUtils.getLeaderForPartition(zkClient, topic, 0)
    assertTrue("Leader for topic new-topic partition 0 should exist", leaderOpt.isDefined)
    val leader = leaderOpt.get

    val messageSet = if(leader == server1.config.brokerId) {
      val response1 = consumer1.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
      response1.messageSet("new-topic", 0).iterator.toBuffer
    }else {
      val response2 = consumer2.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
      response2.messageSet("new-topic", 0).iterator.toBuffer
    }
    assertEquals("Should have fetched 2 messages", 2, messageSet.size)
    assertEquals(new Message(bytes = "test1".getBytes, key = "test".getBytes), messageSet(0).message)
    assertEquals(new Message(bytes = "test2".getBytes, key = "test".getBytes), messageSet(1).message)
    producer1.close()

    try {
      producer2.send(new KeyedMessage[String, String](topic, "test", "test2"))
      fail("Should have timed out for 3 acks.")
    }
    catch {
      case se: FailedToSendMessageException => true
      case e: Throwable => fail("Not expected", e)
    }
    finally {
      producer2.close()
    }
  }


  @Test
  def testSendWithDeadBroker() {
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "kafka.utils.StaticPartitioner")
    props.put("request.timeout.ms", "2000")
    props.put("request.required.acks", "1")
    props.put("metadata.broker.list", TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)))
    props.put("producer.discovery", "static")

    val topic = "new-topic"
    // create topic
    CreateTopicCommand.createTopic(zkClient, topic, 4, 2, "0,0,0,0")
    // waiting for 1 partition is enough
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0, 1000)
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500)
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 1, 500)
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 2, 500)
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 3, 500)

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    try {
      // Available partition ids should be 0, 1, 2 and 3, all lead and hosted only
      // on broker 0
      producer.send(new KeyedMessage[String, String](topic, "test", "test1"))
    } catch {
      case e: Throwable => fail("Unexpected exception: " + e)
    }

    // kill the broker
    server1.shutdown
    server1.awaitShutdown()

    try {
      // These sends should fail since there are no available brokers
      producer.send(new KeyedMessage[String, String](topic, "test", "test1"))
      fail("Should fail since no leader exists for the partition.")
    } catch {
      case e : TestFailedException => throw e // catch and re-throw the failure message
      case e2: Throwable => // otherwise success
    }

    // restart server 1
    server1.startup()
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500)

    try {
      // cross check if broker 1 got the messages
      val response1 = consumer1.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
      val messageSet1 = response1.messageSet(topic, 0).iterator
      assertTrue("Message set should have 1 message", messageSet1.hasNext)
      assertEquals(new Message(bytes = "test1".getBytes, key = "test".getBytes), messageSet1.next.message)
      assertFalse("Message set should have another message", messageSet1.hasNext)
    } catch {
      case e: Exception => fail("Not expected", e)
    }
    producer.close
  }

  @Test
  def testAsyncSendCanCorrectlyFailWithTimeout() {
    val timeoutMs = 500
    val props = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("partitioner.class", "kafka.utils.StaticPartitioner")
    props.put("request.timeout.ms", String.valueOf(timeoutMs))
    props.put("metadata.broker.list", TestUtils.getBrokerListStrFromConfigs(Seq(config1, config2)))
    props.put("producer.discovery", "static")
    props.put("request.required.acks", "1")
    props.put("client.id","ProducerTest-testAsyncSendCanCorrectlyFailWithTimeout")
    props.put("producer.discovery", "static")
    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    val topic = "new-topic"
    // create topics in ZK
    CreateTopicCommand.createTopic(zkClient, topic, 4, 2, "0:1,0:1,0:1,0:1")
    TestUtils.waitUntilMetadataIsPropagated(servers, topic, 0, 1000)
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 500)

    // do a simple test to make sure plumbing is okay
    try {
      // this message should be assigned to partition 0 whose leader is on broker 0
      producer.send(new KeyedMessage[String, String](topic, "test", "test"))
      // cross check if brokers got the messages
      val response1 = consumer1.fetch(new FetchRequestBuilder().addFetch(topic, 0, 0, 10000).build())
      val messageSet1 = response1.messageSet("new-topic", 0).iterator
      assertTrue("Message set should have 1 message", messageSet1.hasNext)
      assertEquals(new Message("test".getBytes), messageSet1.next.message)
    } catch {
      case e: Throwable => case e: Exception => producer.close; fail("Not expected", e)
    }

    // stop IO threads and request handling, but leave networking operational
    // any requests should be accepted and queue up, but not handled
    server1.requestHandlerPool.shutdown()

    val t1 = SystemTime.milliseconds
    try {
      // this message should be assigned to partition 0 whose leader is on broker 0, but
      // broker 0 will not response within timeoutMs millis.
      producer.send(new KeyedMessage[String, String](topic, "test", "test"))
    } catch {
      case e: FailedToSendMessageException => /* success */
      case e: Exception => fail("Not expected", e)
    } finally {
      producer.close
    }
    val t2 = SystemTime.milliseconds

    // make sure we don't wait fewer than numRetries*timeoutMs milliseconds
    // we do this because the DefaultEventHandler retries a number of times
    assertTrue((t2-t1) >= timeoutMs*config.messageSendMaxRetries)
  }
}

