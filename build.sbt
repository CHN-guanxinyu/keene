

import scala.sys.process.Process

val gitHeadCommitSha = taskKey[String]("determine the current git commit SHA")
val makeVersionProperties = taskKey[Seq[File]]("make a version.properties file.")

inThisBuild( gitHeadCommitSha := Process("git rev-parse HEAD").lineStream.head )

lazy val keene = preownedKittenProject("keene" , ".").
  settings(
    makeVersionProperties := {
      val propFile = (resourceManaged in Compile).value / "version.properties"
      val content = s"version=${gitHeadCommitSha.value}"
      IO.write( propFile , content )
      Seq(propFile)
    },
    onLoadMessage ~= ( _ + ( if( (sys props "java.specification.version") < Version.min.jdk ) {
      s"""
         |You seem to not be running Java ${Version.min.jdk}.
         |While the provided code may still work, we recommend that you
         |upgrade your version of Java.
    """.stripMargin
    }else "" )),

    libraryDependencies ++= Dependencies.core,
    libraryDependencies ++= Lib.spark.all,
    libraryDependencies ++= Seq(
      Lib.kafka,
      Lib.spark.streaming_kafka
    )
  ).settings(CommonSetting.projectSettings)




/**
  * 创建通用模板
  */
def preownedKittenProject(name : String , path : String ) : Project ={
  Project( name , file(path) ).
    settings(
      version := "0.1-SNAPSHOT",
      organization := "com.keene",
      scalaVersion := Version.scala,
      test in assembly := {}
    ).enablePlugins(AssemblyPlugin)
}