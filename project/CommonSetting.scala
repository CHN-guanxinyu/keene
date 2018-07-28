import sbt.Keys._
import sbt._
import sbt.plugins.JvmPlugin

object CommonSetting extends AutoPlugin {
  override def requires = JvmPlugin

  override def trigger = allRequirements

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    parallelExecution in Test := false,

    fork in Test := true,
    fork in run := true
  )
}
