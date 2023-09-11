name := "Insert-Ref"

version := "2.0"

scalaVersion := "2.12.10"

fork := true

val sparkVersion = "3.1.3"

//scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked", "-encoding", "utf8")

mainClass in (packageBin) := Some("org.novakorp.com.entry")

libraryDependencies ++= Seq("org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  ,"org.apache.spark" %% "spark-core" % sparkVersion % "provided"
  ,"org.apache.spark" %% "spark-yarn" % sparkVersion
  ,"org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  ,"org.scalatest" %% "scalatest" % "3.2.10" % Test
)

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")

lazy val myProject = project.enablePlugins(DeploySSH)