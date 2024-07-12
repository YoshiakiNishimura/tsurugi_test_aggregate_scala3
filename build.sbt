val scala3Version = "3.4.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "tsurugi_test_aggregate_sbt",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.0.0" % Test,
      "com.tsurugidb.tsubakuro" % "tsubakuro-session" % "1.3.0",
      "com.tsurugidb.tsubakuro" % "tsubakuro-connector" % "1.3.0",
      "com.tsurugidb.tsubakuro" % "tsubakuro-kvs" % "1.3.0",
      "com.tsurugidb.iceaxe" % "iceaxe-core" % "1.3.0",
      "org.slf4j" % "slf4j-simple" % "1.7.32"
    )
  )
