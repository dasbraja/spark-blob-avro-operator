name := "spark-blob-avro-operator"

version := "0.1"

scalaVersion := "2.12.7"
val sparkVersion = "2.4.5"
scalacOptions ++= Seq("-deprecation", "-feature")

//libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % Test
libraryDependencies += "com.typesafe" % "config" % "1.4.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark"  %% "spark-avro" % sparkVersion
libraryDependencies += ("org.apache.hadoop" % "hadoop-azure" % "2.10.1")
                          .excludeAll(ExclusionRule(organization = "com.sun.jersey"))





javaOptions in Test += s"-Dconfig.file=${sourceDirectory.value}/test/resources/reference.test.conf"
fork in Test := true


assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x if x.endsWith("mime.types") => MergeStrategy.last
  case x if x.endsWith("module-info.class") => MergeStrategy.last
  case x if x.endsWith("git.properties") => MergeStrategy.last
  case x if x.endsWith("overview.html") => MergeStrategy.last
//  case PathList("javax", "activation", xs @ _*)     => MergeStrategy.first
//  case PathList("net",   "jpountz", _*) => MergeStrategy.last
//  case PathList("io",    "netty", _*) => MergeStrategy.last
  case PathList("org",   "slf4j", _*) => MergeStrategy.last
//  case PathList("org",   "json4s-ast", _*) => MergeStrategy.last

  case PathList("org",   "apache", _*) => MergeStrategy.last
//  case PathList("org",   "apache", "spark", _*) => MergeStrategy.last
  case PathList("org",   "aopalliance", xs @ _*) => MergeStrategy.last
//  case PathList("javax", "xml", xs @ _*)     => MergeStrategy.first
  case PathList("javax", "ws", xs @ _*) => MergeStrategy.last
  case PathList("com",   "sun", "jersey", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
//  case PathList("com",   "sun", xs @ _*) => MergeStrategy.last
  case PathList("logback.xml",  xs @ _*) => MergeStrategy.first


  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

/*
coverageMinimum := 90
coverageFailOnMinimum := true
coverageHighlighting := true
publishArtifact in Test := false
parallelExecution in Test := false
*/