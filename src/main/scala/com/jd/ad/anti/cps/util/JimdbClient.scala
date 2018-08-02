package com.jd.ad.anti.cps.util

import com.jd.jim.cli.{Cluster, ReloadableJimClientFactory}

object JimdbClient extends Serializable {
  @transient private var client_staging: Cluster = null
  @transient private var client_online: Cluster = null
  def getCluster(conf: JobIni): Cluster = {
    getCluster(conf, conf.getRunEnv())
  }

  def getCluster(conf: JobIni, env: String): Cluster = {
    if (env.equalsIgnoreCase("online")) {
      if (null == client_online) {
        val clientFactory: ReloadableJimClientFactory = new ReloadableJimClientFactory()
        clientFactory.setJimUrl(conf.getJimdbOnlineURL())
        client_online = clientFactory.getClient()
      }
      client_online
    }else {
      if (null == client_staging) {
        val clientFactory: ReloadableJimClientFactory = new ReloadableJimClientFactory()
        clientFactory.setJimUrl(conf.getJimdbStagingURL())
        client_staging = clientFactory.getClient()
      }
      client_staging
    }
  }
}