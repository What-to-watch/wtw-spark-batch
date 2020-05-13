name := "wtw-spark-batch"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.5"
val zioVersion = "1.0.0-RC18-2"

libraryDependencies += "dev.zio" %% "zio" % zioVersion

libraryDependencies += "com.softwaremill.sttp.client" %% "okhttp-backend" % "2.1.1"
libraryDependencies += "com.softwaremill.sttp.client" %% "circe" % "2.1.1"
// https://mvnrepository.com/artifact/io.circe/circe-generic
libraryDependencies += "io.circe" %% "circe-generic" % "0.12.0-M3"


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
// https://mvnrepository.com/artifact/org.postgresql/postgresql
//libraryDependencies += "org.postgresql" % "postgresql" % "42.2.12"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyMergeStrategy in assembly := {
  case "META-INF\\io.netty.versions.properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
