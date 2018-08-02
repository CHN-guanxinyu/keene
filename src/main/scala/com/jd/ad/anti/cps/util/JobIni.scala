package com.jd.ad.anti.cps.util

import java.io.FileInputStream

import org.ini4j.Ini
import org.ini4j.Profile.Section
import scala.collection.convert.WrapAsScala
import scala.collection.mutable.Map
/**
  * Created by haojun on 2017/9/4.
  */
class JobIni extends Serializable {

  val JOB_RUN_ENV="job.run.env"
  val JOB_NAME = "job.name"
  val JOB_KAFKA_BROKERS = "job.kafka.brokers"
  val JOB_FREQUENCY_S = "job.frequency.seconds"
  val JOB_KAFKA_CLICK_TOPICS = "job.kafka.click.topics"
  val JOB_CONF_PATH = "job.conf.path"
  val OUTPUT_HDFS_ROOT_PATH = "output.hdfs.root.path"
  val YAML_FILE = "yaml.file"
  val VAR_CACHE_UPDATE_DURATION = "variable.cache.update.duration"
  val JOB_CHECK_POINT_ENABLE = "job.check.point.enable"
  val JOB_CHECK_POINT_RESTORE_ON_START = "job.check.point.restore.onstart"
  val JOB_CHECK_POINT_PATH = "job.check.point.path"
  val VARIABLES_UNION_WEB_PATH = "variables.union.web.path"
  val VARIABLES_UNION_WEB_SOCIAL_PATH = "variables.union.web.social.path"
  val VARIABLES_THIRD_PARTY_UNION_PATH = "variables.third.party.union.path"
  val VARIABLES_NOREFER_WHITE_LIST_PATH = "variables.norefer.white.list.path"
  val VARIABLES_UNION_BLACK_LIST_PATH = "variables.union.black.list.path"
  val VAR_WINDOW_DURATION_S = "variable.window.duration.seconds"
  val VAR_WINDOW_SLIDE_DURATION_S = "variable.window.slide.duration.seconds"
  val UNION_FRAUD_CLICK_OUTPUT_PATH = "union.fraud.click.output.path"
  val UNION_FRAUD_ORDER_OUTPUT_PATH = "union.fraud.order.output.path"
  val CACHE_FRAUD_CLICK_WINDOW_DURATION = "cache.fraud.click.window.duration"
  val CACHE_FRAUD_CLICK_WINDOW_SLIDE_DURATION = "cache.fraud.click.window.slide.duration"
  // jmq
  val JOB_JMQ_CONSUMER_TOPIC = "job.jmq.consumer.topic"
  val JOB_JMQ_PRODUCER_TOPIC = "job.jmq.producer.topic"
  val JOB_JMQ_APP = "job.jmq.app"
  val JOB_JMQ_ADDRESS = "job.jmq.address"
  val JOB_JMQ_USER = "job.jmq.user"
  val JOB_JMQ_PASSWORD = "job.jmq.password"
  val JOB_JMQ_SENDTIMEOUT = "job.jmq.sendtimeout"
  val JOB_JMQ_EPOLL = "job.jmq.epoll"
  val JOB_JMQ_SEND_ENABLED = "job.jmq.send.enabled"
  val JOB_JMQ_SEND_RETRY_TIMES = "job.jmq.send.retry.times"
  // order kafka 
  val JOB_KAFKA_ORDER_TOPICS = "job.kafka.order.topics"
  
  // kafka三号集群配置
  // 配置文件路径
  val JOB_KAFKA_SECURITY_AUTH_LOGIN_CONFIG = "job.kafka.security.auth.login.config"
  // 设置权限协议以及认证
  val JOB_KAFKA_SECURITY_PROTOCOL = "job.kafka.security.protocol"
  val JOB_KAFKA_SECURITY_MECHANISM = "job.kafka.security.mechanism"
  val JOB_KAFKA_ZOOKEEPER_CONNECT = "job.kafka.zookeeper.connect"
  val JOB_KAFKA_GROUP_ID = "job.kafka.group.id"
  val JOB_KAFKA_KEY_SERIALIZER = "job.kafka.key.serializer"
  val JOB_KAFKA_VALUE_SERIALIZER = "job.kafka.value.serializer"
  val JOB_KAFKA_KEY_DESERIALIZER = "job.kafka.key.deserializer"
  val JOB_KAFKA_VALUE_DESERIALIZER = "job.kafka.value.deserializer"
  
  // jimdb
  val JOB_JIMDB_STAGING_URL = "job.jimdb.staging.url"
  val JOB_JIMDB_ONLINE_URL = "job.jimdb.online.url"
  val JOB_JIMDB_ORDER_EXPIRE_DAY = "job.jimdb.order.expire.day"
  val JOB_JIMDB_ORDER_KEY_PREFIX = "job.jimdb.order.key.prefix"
  val CLICK_EXPIRE_TIME = "click.expire.time"
  val PREFIX_CLICK = "click.prefix"
  val IS_SET_CLICK = "is.set.click"

  // monitor
  val MONITOR_URL = "monitor.url"
  val MONITOR_DB = "monitor.db"
  val MONITOR_TABLE = "monitor.table"
  val MONITOR_FIELDS = "monitor.fields"
  val MONITOR_USER = "monitor.user"
  val MONITOR_TOKEN = "monitor.token"
  val MONITOR_TAG = "monitor.tag"
  val MONITOR_STATUS_TAG = "monitor.status.tag"
  val MONITOR_ENABLED = "monitor.enabled"
  
  
  var props: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()

  def this(fileName: String) {
    this()
    var is: FileInputStream = null
    try {
      is = new FileInputStream(fileName)
      val ini = new Ini(is)
      val section: Section = ini.get("common")
      if (section != null) WrapAsScala.asScalaSet(section.keySet()).foreach(k => props.put(k, section.get(k)))
    } finally {
      is.close()
    }
  }

  def set(k: String, v: String): Unit = {
    props.put(k, v)
  }

  def getOrException(k: String): String = {
    props.get(k) match {
      case Some(v) => v
      case _ => throw new IllegalArgumentException("No value for key:" + k)
    }
  }

  def getOrPut(k: String, v: String): String = {
    props.getOrElse(k, v)
  }

  def getRunEnv(): String = getOrException(JOB_RUN_ENV)

  def getJobName(): String = getOrException(JOB_NAME)

  def getJobFrequency(): Int = getOrException(JOB_FREQUENCY_S).toInt

  def getKafkaBrokers(): String = getOrException(JOB_KAFKA_BROKERS)

  def getKafkaClickTopics(): String = getOrException(JOB_KAFKA_CLICK_TOPICS)
  
  def getKafkaOrderTopics(): String = getOrException(JOB_KAFKA_ORDER_TOPICS)
  
  def getKafkaSecurityAuthLoginConfig(): String = getOrException(JOB_KAFKA_SECURITY_AUTH_LOGIN_CONFIG)
  
  def getKafkaSecurityProtocol(): String = getOrException(JOB_KAFKA_SECURITY_PROTOCOL)
  
  def getKafkaSecurityMechanism(): String = getOrException(JOB_KAFKA_SECURITY_MECHANISM)
  
  def getKafkaZookeeperConnect(): String = getOrException(JOB_KAFKA_ZOOKEEPER_CONNECT)
  
  def getKafkaGroupId(): String = getOrException(JOB_KAFKA_GROUP_ID)
  
  def getKafkaKeySerializer(): String = getOrException(JOB_KAFKA_KEY_SERIALIZER)
  
  def getKafkaValueSerializer(): String = getOrException(JOB_KAFKA_VALUE_SERIALIZER)
  
  def getKafkaKeyDeserializer(): String = getOrException(JOB_KAFKA_KEY_DESERIALIZER)
  
  def getKafkaValueDeserializer(): String = getOrException(JOB_KAFKA_VALUE_DESERIALIZER)

  def getYamlFile(): String = getOrException(YAML_FILE)

  def getCacheUpdateDuration(): Int = getOrException(VAR_CACHE_UPDATE_DURATION).toInt

  def isCheckPointEnable(): Boolean = getOrPut(JOB_CHECK_POINT_ENABLE, "false").toBoolean

  def isRestoreFromCheckPoint(): Boolean = getOrPut(JOB_CHECK_POINT_RESTORE_ON_START, "false").toBoolean

  def getHdfsRootPath(): String = getOrException(OUTPUT_HDFS_ROOT_PATH)

  def getCheckPointPath(): String = getHdfsRootPath()+ "/" + getOrException(JOB_CHECK_POINT_PATH)

  def getVarUnionWebPath(): String = getOrException(VARIABLES_UNION_WEB_PATH)

  def getVarUnionWebSocialPath(): String = getOrException(VARIABLES_UNION_WEB_SOCIAL_PATH)

  def getVarThirdPartyUnionPath(): String = getOrException(VARIABLES_THIRD_PARTY_UNION_PATH)

  def getVarNoReferWhiteListPath(): String = getOrException(VARIABLES_NOREFER_WHITE_LIST_PATH)

  def getVarUnionBlackListPath(): String = getOrException(VARIABLES_UNION_BLACK_LIST_PATH)

  def getWindowDuration(): Long = getOrException(VAR_WINDOW_DURATION_S).toLong * 1000

  def getWindowSlideDuration(): Long = getOrException(VAR_WINDOW_SLIDE_DURATION_S).toLong * 1000

  def getUnionFraudClickOutputPath(): String = getHdfsRootPath()+ "/" + getOrException(UNION_FRAUD_CLICK_OUTPUT_PATH)

  def getUnionFraudOrderOutputPath(): String = getHdfsRootPath()+ "/" + getOrException(UNION_FRAUD_ORDER_OUTPUT_PATH)

  def getJobConfPath(): String = getOrException(JOB_CONF_PATH)
    
  def getJobJMQConsumerTopic(): String = getOrException(JOB_JMQ_CONSUMER_TOPIC)
  
  def getJobJMQProducerTopic(): String = getOrException(JOB_JMQ_PRODUCER_TOPIC)
  
  def getJobJMQApp(): String = getOrException(JOB_JMQ_APP)
  
  def getJobJMQAddress(): String = getOrException(JOB_JMQ_ADDRESS)
  
  def getJobJMQUser(): String = getOrException(JOB_JMQ_USER)
  
  def getJobJMQPassword(): String = getOrException(JOB_JMQ_PASSWORD)
  
  def getJobJMQSendtimeout(): Int = getOrException(JOB_JMQ_SENDTIMEOUT).toInt
  
  def getJobJMQEpoll(): Boolean = getOrPut(JOB_JMQ_EPOLL, "false").toBoolean

  def getJobJMQSendEnabled(): Boolean = getOrPut(JOB_JMQ_SEND_ENABLED, "false").toBoolean

  def getCacheFraudClickWindowDuration(): Int = getOrException(CACHE_FRAUD_CLICK_WINDOW_DURATION).toInt

  def getCacheFraudClickWindowSlideDuration(): Int = getOrException(CACHE_FRAUD_CLICK_WINDOW_SLIDE_DURATION).toInt
  
  def getJimdbStagingURL(): String = getOrException(JOB_JIMDB_STAGING_URL)

  def getJimdbOnlineURL(): String = getOrException(JOB_JIMDB_ONLINE_URL)

  def getJimdbOrderExpireDay(): Int = getOrPut(JOB_JIMDB_ORDER_EXPIRE_DAY, "7").toInt

  def getJimdbOrderKeyPrefix(): String = getOrPut(JOB_JIMDB_ORDER_KEY_PREFIX, "send_")

  def getClickPrefix(): String = getOrException(PREFIX_CLICK)

  def getClickExpireTime(): Long = getOrPut(CLICK_EXPIRE_TIME, "1296000").toLong

  def getIsSetClick(): Boolean = getOrPut(IS_SET_CLICK, "true").toBoolean
  
  def getMonitorUrl(): String = getOrException(MONITOR_URL)

  def getMonitorDb(): String = getOrException(MONITOR_DB)

  def getMonitorTable(): String = getOrException(MONITOR_TABLE)

  def getMonitorFields(): String = getOrException(MONITOR_FIELDS)

  def getMonitorUser(): String = getOrException(MONITOR_USER)

  def getMonitorToken(): String = getOrException(MONITOR_TOKEN)

  def getMonitorTag():String= getOrException(MONITOR_TAG)
  
  def getMonitorStatus(): Boolean = getOrException(MONITOR_STATUS_TAG).toBoolean

  def getMonitorEnabled(): Boolean = getOrPut(MONITOR_ENABLED, "true").toBoolean

  def getJobJmqSendRetryTimes(): Int = getOrPut(JOB_JMQ_SEND_RETRY_TIMES, "3").toInt
}
