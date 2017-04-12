import sbt._
import Keys._
import sbt.Project.projectToRef
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._

lazy val appVersion = "0.0.1"
lazy val scala = "2.11.8"

lazy val versions = new {
  val akka = "2.4.16"
  val camel = "2.13.4"
  val activeMQ = "5.14.0"
  val spark = "2.0.0"
  val hadoop = "2.7.0"
  val psqlJdbc = "9.4-1206-jdbc41"
  val ficus = "1.2.6"
  val slf4j = "1.7.16"
  val logback = "1.1.3"
  val guice = "4.1.0"
}

lazy val commonSettings = Seq(
  version := appVersion,
  scalaVersion := scala,
  scalacOptions ++= scalaCompilerOptions,
  ScalariformKeys.preferences := ScalariformKeys.preferences.value
    .setPreference(FormatXml, false)
    .setPreference(DoubleIndentClassDeclaration, false)
    .setPreference(DanglingCloseParenthesis, Preserve)
    .setPreference(AlignParameters, false)
    .setPreference(CompactStringConcatenation, false)
    .setPreference(IndentPackageBlocks, true)
    .setPreference(PreserveSpaceBeforeArguments, false)
    .setPreference(RewriteArrowSymbols, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 40)
    .setPreference(SpaceBeforeColon, false)
    .setPreference(SpaceInsideBrackets, false)
    .setPreference(SpaceInsideParentheses, false)
    .setPreference(IndentSpaces, 2)
    .setPreference(IndentLocalDefs, false)
    .setPreference(SpacesWithinPatternBinders, true)
    .setPreference(SpacesAroundMultiImports, true),
  excludeFilter in scalariformFormat := (excludeFilter in scalariformFormat).value ||
    "Routes.scala" ||
    "ReverseRoutes.scala" ||
    "JavaScriptReverseRoutes.scala" ||
    "RoutesPrefix.scala"
)

lazy val SubsistemDisposisiOtomatis = (project in file("."))
  .settings(SbtScalariform.defaultScalariformSettings: _*)
  .settings(commonSettings: _*)
  .settings(
    name := "subsistem-disposisi-otomatis",
    fork in run := false,
    connectInput in run := true,
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % versions.akka,
      "com.typesafe.akka" %% "akka-camel" % versions.akka,
      "org.apache.camel" % "camel-jetty" % versions.camel,
      "org.apache.activemq" % "activemq-camel" % versions.activeMQ,
      "org.apache.spark" %% "spark-core" % versions.spark,
      "org.apache.spark" %% "spark-sql" % versions.spark,
      "org.apache.spark" %% "spark-mllib" % versions.spark,
      "org.apache.spark" %% "spark-streaming" % versions.spark,
      "org.apache.hadoop" % "hadoop-client" % versions.hadoop,
      "org.postgresql" % "postgresql" % versions.psqlJdbc,
      "com.iheart" %% "ficus" % versions.ficus,
      "org.slf4j" % "slf4j-api" % versions.slf4j,
      "ch.qos.logback" % "logback-classic" % versions.logback,
      "net.codingwell" %% "scala-guice" % versions.guice
    ).map(_.exclude("org.slf4j", "slf4j-log4j12"))
  ).enablePlugins(SbtScalariform)

lazy val jvmOptions = Seq(
  "-Xms256M",
  "-Xmx2G",
  "-XX:MaxPermSize=2048M",
  "-XX:+UseConcMarkSweepGC",
  "-Dorg.apache.activemq.SERIALIZABLE_PACKAGES=*"
)

lazy val scalaCompilerOptions = Seq(
  "-deprecation", // Emit warning and location for usages of deprecated APIs.
  "-feature", // Emit warning and location for usages of features that should be imported explicitly.
  "-unchecked", // Enable additional warnings where generated code depends on assumptions.
  "-Xfatal-warnings", // Fail the compilation if there are any warnings.
  "-Xlint", // Enable recommended additional warnings.
  "-Ywarn-adapted-args", // Warn if an argument list is modified to match the receiver.
  "-Ywarn-dead-code", // Warn when dead code is identified.
  "-Ywarn-inaccessible", // Warn about inaccessible types in method signatures.
  "-Ywarn-nullary-override", // Warn when non-nullary overrides nullary, e.g. def foo() over def foo.
  "-Ywarn-numeric-widen" // Warn when numerics are widened.
)