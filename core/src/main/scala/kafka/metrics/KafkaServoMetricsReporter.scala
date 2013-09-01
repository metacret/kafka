package kafka.metrics

import kafka.metrics.{KafkaMetricsReporterMBean, KafkaMetricsConfig, KafkaMetricsReporter}
import kafka.utils.{VerifiableProperties, Logging}
import com.yammer.metrics.Metrics

import java.util.concurrent.TimeUnit
import com.netflix.nfkafka.metrics.ServoReporter

private trait KafkaServoMetricsReporterMBean extends KafkaMetricsReporterMBean

private class KafkaServoMetricsReporter extends KafkaMetricsReporter
                                with KafkaServoMetricsReporterMBean
                                with Logging {
  private var underlying: ServoReporter = null
  private var running = false
  private var initialized = false


  override def getMBeanName = "kafka:type=kafka.metrics.KafkaServoMetricsReporter"


  override def init(props: VerifiableProperties) {
    synchronized {
      if (!initialized) {
        val metricsConfig = new KafkaMetricsConfig(props)
        underlying = new ServoReporter(Metrics.defaultRegistry())
        initialized = true
        startReporter(metricsConfig.pollingIntervalSecs)
      }
    }
  }


  override def startReporter(pollingPeriodSecs: Long) {
    synchronized {
      if (initialized && !running) {
        underlying.start(pollingPeriodSecs, TimeUnit.SECONDS)
        running = true
        info("Started Kafka Servo metrics reporter with polling period %d seconds".format(pollingPeriodSecs))
      }
    }
  }


  override def stopReporter() {
    synchronized {
      if (initialized && running) {
        underlying.shutdown()
        running = false
        info("Stopped Kafka CSV metrics reporter")
        underlying = new ServoReporter(Metrics.defaultRegistry())
      }
    }
  }

}

