import sbt._
import sbt.Keys._

object Dependencies {
  val core = Seq(
    Lib.lombok,
//    Lib.scalaCheck,
    Lib.slf4j_impl,
    Lib.log4j_api,
    Lib.scalactic,
    Lib.scalaTest,
    Lib.commons_codec
  )
}




object Version{


  val commons_codec = "1.10"

  //scala
  val scala = "2.11.8"
  val scalaTest = "3.0.5"
  val scalaCheck = "1.13.0"
  val scalactic = "3.0.5"
  //spark
  val spark = "2.3.0"

  //hadoop
  val hadoop = "2.5.2"

  val kafka = "1.1.1"

  //hbase
  val hbase = "1.0.2"
  val hbase_spark_connr = hbase


  val lombok = "1.16.20"

  val log4j = "2.11.0"
  val slf4j = "1.7.7"

  val jackson = "2.6.2"

  object min{
    val jdk = "1.8"
  }
}








object Lib{

  val commons_codec   = "commons-codec"               % "commons-codec"               % Version.commons_codec

  val lombok          = "org.projectlombok"           % "lombok"                      % Version.lombok        % "provided"
  val scalaCheck      = "org.scalacheck"              %% "scalacheck"                 % Version.scalaCheck
  val scalaTest       = "org.scalatest"               %% "scalatest"                  % Version.scalaTest     % "test"
  val scalactic       = "org.scalactic"               %% "scalactic"                  % Version.scalactic

  val slf4j_api       = "org.slf4j"                   % "slf4j-api"                   % Version.slf4j
  val slf4j_impl      = "org.apache.logging.log4j"    % "log4j-slf4j-impl"            % Version.log4j
  val log4j_api       = "org.apache.logging.log4j"    % "log4j-api"                   % Version.log4j

  object jackson {
    val core          = "com.fasterxml.jackson.core"  % "jackson-core"                % Version.jackson
    val databind      = "com.fasterxml.jackson.core"  % "jackson-databind"            % Version.jackson
    val mudule_scala  = "com.fasterxml.jackson.module"% "jackson-module-scala_2.11"   % Version.jackson
  }



  object spark{
    private def foo( md : String ) = "org.apache.spark" %% s"spark-$md" % Version.spark 

    val core            = foo("core") excludeAll ExclusionRule(organization="commons-codec")
    val sql             = foo("sql")
    val mllib           = foo("mllib")
    val graphx          = foo("graphx")
    val streaming       = foo("streaming")
    val streaming_kafka = foo("streaming-kafka-0-10")

    val all = Seq( core , sql , mllib , graphx , streaming )
  }

  object hbase{
    private def foo( md : String )="org.apache.hbase"   %s"hbase-$md"  % Version.hbase % "provided" excludeAll(
      ExclusionRule(
        organization = "javax.servlet",
        name="javax.servlet-api"
      ),
      ExclusionRule(
        organization = "org.mortbay.jetty",
        name="jetty"
      ),
      ExclusionRule(
        organization = "org.mortbay.jetty",
        name="servlet-api-2.5"
      )
    )

    val client          = foo("client")
    val common          = foo("common")
    val server          = foo("server")
    val protocol        = foo("protocol")
    val hadoop2_compat  = foo("hadoop2-compat")
    val hadoop_compat   = foo("hadoop-compat")

    val spark_connector = "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % Version.hbase_spark_connr excludeAll(
      ExclusionRule(organization="org.apache.logging.log4j"),
      ExclusionRule(organization="org.slf4j"),
    )

    val all = Seq( common , client , server , protocol , hadoop2_compat , hadoop_compat , spark_connector )
  }

  val hadoop_client = "org.apache.hadoop" % "hadoop-client" % Version.hadoop excludeAll(
    ExclusionRule(organization="joda-time"),
    ExclusionRule(organization="org.slf4j")
  )

  val kafka = "org.apache.kafka" %% "kafka" % Version.kafka excludeAll ExclusionRule("com.fasterxml.jackson.core")

//  val scalajs_dom =


}