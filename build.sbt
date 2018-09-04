
lazy val keene = preownedKittenProject("keene" , ".").
  settings(
    onLoadMessage ~= ( _ + ( if( (sys props "java.specification.version") < Version.min.jdk ) {
      s"""
         |You seem to not be running Java ${Version.min.jdk}.
         |While the provided code may still work, we recommend that you
         |upgrade your version of Java.
    """.stripMargin
    }else "" )),

    libraryDependencies ++= Lib.spark.all,
    libraryDependencies ++= Lib.akka.all,
    libraryDependencies ++= Seq(
      Lib.kafka,
      Lib.spark.streaming_kafka,
      Lib.spark.sql_kafka,

      Lib.ini4j
    )
  ).settings(CommonSetting.projectSettings)




/**
  * 创建通用模板
  */
def preownedKittenProject(name : String , path : String ) : Project ={
  Project( name , file(path) ).
    settings(
      version := "1.2-SNAPSHOT",
      organization := "com.keene",
      scalaVersion := Version.scala,
      test in assembly := {}
    ).enablePlugins(AssemblyPlugin)
}