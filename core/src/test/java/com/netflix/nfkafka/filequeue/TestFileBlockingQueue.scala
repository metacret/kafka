package com.netflix.nfkafka.filequeue

import org.junit.Assert._
import org.junit.{Test, After, Before}
import kafka.producer.KeyedMessage

class TestFileBlockingQueue {
  private val queue: FileBlockingQueue[Long, Array[Byte]] =
    new FileBlockingQueue[Long, Array[Byte]]("build/test/fileblockingqueue", 1, new DefaultSerDe)

  @Before
  @After def clear {
    queue.clear
  }

  @Test def testSerDe {
    val m: KeyedMessage[Long, Array[Byte]] = new KeyedMessage[Long, Array[Byte]]("topic", 1L, "message".getBytes)
    val buffer: Array[Byte] = queue.serialize(m)
    val dm: KeyedMessage[Long, Array[Byte]] = queue.deserialize(buffer)
    assertEquals(dm.topic, "topic")
    assertEquals(dm.key.asInstanceOf[Long], 1L)
    assertArrayEquals(dm.message, "message".getBytes)

    val shutdownCommand = new KeyedMessage[Long,Array[Byte]]("shutdown", null.asInstanceOf[Long], null.asInstanceOf[Array[Byte]])
    queue.put(shutdownCommand)
    val scdm = queue.poll()
    assertEquals(shutdownCommand, scdm)
  }

  @Test def testQueue {
    var i: Int = 0
    while (i < 2 * Byte.MaxValue) {
      queue.offer(new KeyedMessage[Long, Array[Byte]]("topic", i.asInstanceOf[Long], ("message" + i).getBytes))
      i += 1; i
    }
    queue.offer(new KeyedMessage[Long, Array[Byte]]("shutdown", null.asInstanceOf[Long], null.asInstanceOf[Array[Byte]]))
    i = 0
    while (i < 2 * Byte.MaxValue) {
      val m: KeyedMessage[Long, Array[Byte]] = queue.poll
      assertEquals(m.topic, "topic")
      assertEquals(m.key.byteValue, i.toByte)
      assertArrayEquals(m.message, ("message" + i).getBytes)
      i += 1; i
    }

    val m: KeyedMessage[Long, Array[Byte]] = queue.poll
    assertEquals(m.topic, "shutdown")
    assertNull(m.key)
    assertNull(m.message)
    assertEquals(queue.size, 0)
  }
}
