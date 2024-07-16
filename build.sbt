val scala3Version = "3.4.2"
val tsurugiVersion = "1.3.0"
lazy val root = project
  .in(file("."))
  .settings(
    name := "tsurugi_test_aggregate_sbt",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % "1.0.0" % Test,
      "com.tsurugidb.tsubakuro" % "tsubakuro-session" % tsurugiVersion,
      "com.tsurugidb.tsubakuro" % "tsubakuro-connector" % tsurugiVersion,
      "com.tsurugidb.tsubakuro" % "tsubakuro-kvs" %  tsurugiVersion,
      "com.tsurugidb.iceaxe" % "iceaxe-core" %  tsurugiVersion,
      "org.slf4j" % "slf4j-simple" % "1.7.32"
    )
  )
