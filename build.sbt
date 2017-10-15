import Dependencies._


cancelable in Global := true

val AkkaVersion = "2.5.6"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.2",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "alpakka-ampq-retry-archive",
    resolvers += Resolver.bintrayRepo("akka", "maven"),
    libraryDependencies += "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-amqp" % "0.14",
    libraryDependencies += scalaTest % Test
  )
