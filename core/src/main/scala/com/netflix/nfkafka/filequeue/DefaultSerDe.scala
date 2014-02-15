package com.netflix.nfkafka.filequeue

class DefaultSerDe extends SerDe[Long, Array[Byte]] {
  def getKeyFromBytes(payload: Array[Byte]): Long = {
    payload(0).asInstanceOf[Long]
  }

  def getValueFromBytes(payload: Array[Byte]): Array[Byte] = {
    payload
  }

  def toBytesKey(key: Long): Array[Byte] = {
    if (key == null.asInstanceOf[Long]) {
      null.asInstanceOf[Array[Byte]]
    } else {
      val bytesKey: Array[Byte] = new Array[Byte](1)
      bytesKey(0) = key.byteValue
      bytesKey
    }
  }

  def toBytesValue(value: Array[Byte]): Array[Byte] = {
    value
  }
}
