import V._

lazy val globalScalacOptions = Seq(
  "-encoding", "UTF-8"
  , "-g:vars"
  , "-feature"
  , "-unchecked"
  , "-deprecation"
  , "-target:jvm-1.8"
  , "-Xlog-reflective-calls"
  ,"-Ywarn-unused:-imports"
  , "-Ywarn-value-discard"
)

val commonBuildSettings =
    Defaults.coreDefaultSettings ++
      Seq(
        organization := "panalgo",
        version := V.panalgo,
        scalaVersion := V.scala,
        scalacOptions := globalScalacOptions,
        scalacOptions in (Compile, console) := globalScalacOptions,
        scalacOptions in (Test, console) := globalScalacOptions,
        fork in Test := true,
        javaOptions in Test ++= Seq("-Xmx4g", "-Xdebug", "-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005"),
        parallelExecution := false
      )

lazy val root = Project(id = "panalgo-root", base = file("."))
  .aggregate(panalgo)
  .settings(scalacOptions := globalScalacOptions)
  .settings(scalaVersion := V.scala)
  .settings(name := "panalgo-root")

lazy val panalgo = Project(id = "panalgo", base = file("panalgo"))
  .settings(commonBuildSettings: _*)
  .settings(excludeDependencies += "commons-logging" % "commons-logging")
  .settings(
    name := "panalgo",
    libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % V.spark % "provided",
    "org.apache.spark" %% "spark-sql" % V.spark % "provided",
    "com.databricks" %% "spark-xml" % V.sparkXml,

      "org.scalatest" %% "scalatest" % V.scalaTest,
      "org.scalatest" %% "scalatest-funsuite" % V.scalaTest,
      "org.scalatest" %% "scalatest-flatspec" % V.scalaTest,
      "org.scalatest" %% "scalatest-mustmatchers" % V.scalaTest,
      "org.scalatest" %% "scalatest-shouldmatchers" % V.scalaTest
    ))
  .settings(
    mainClass in assembly := Some("panalgo.Panalgo"),
    assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  })
