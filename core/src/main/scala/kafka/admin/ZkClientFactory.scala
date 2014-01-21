package kafka.admin

import org.I0Itec.zkclient.ZkClient
import java.util.concurrent.atomic.AtomicReference
import kafka.utils.ZKStringSerializer

object ZkClientFactory {
  val zkClient = new AtomicReference[ZkClient]

  def get(zkConnect: String) : ZkClient = {
    if (zkClient.get() == null)
      zkClient.set(new ZkClient(zkConnect, 20000, 20000, ZKStringSerializer))
    zkClient.get()
  }
}
