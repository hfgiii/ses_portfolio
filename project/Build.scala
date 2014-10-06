import sbt.Keys._
import sbt._

object ESPortfolioBuild extends Build {
  import Deps._
  import Reps._



  val buildOrganization = "org.hfgiii.sesportfolio"
  val buildVersion = "0.1.0-SNAPSHOT"
  val buildPublishTo = None
  val buildScalaVersion = "2.11.2"

  val buildParentName = "parent"

  val BaseSettings = Project.defaultSettings ++ Seq(
    organization := buildOrganization,
    publishTo := buildPublishTo,
    scalaVersion := buildScalaVersion,
    crossScalaVersions := Seq("2.10.2", "2.11.2"),
    scalacOptions := Seq("-unchecked", "-deprecation", "-feature"),
    resolvers := reps)

  def sesPortfolioProject(projectName: String): Project = {
    Project(
      id = projectName,
      base = file(projectName),
      settings = BaseSettings ++ Seq(
        name := projectName,
        version := buildVersion))
  }

  lazy val root = Project(
    id = buildParentName,
    base = file("."),
    settings = BaseSettings) aggregate(analytics4sProject,core4sProject)


  lazy val core4sProject =
    sesPortfolioProject("core").
      settings(libraryDependencies := deps ++  testingDeps)

  lazy val analytics4sProject =
    sesPortfolioProject("analytics").dependsOn(core4sProject).
      settings(libraryDependencies := deps ++  testingDeps)


}

object Reps {
  val reps = Seq(
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Sonatype OSS" at "https://oss.sonatype.org/content/repositories/releases/",
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "gphat" at "https://raw.github.com/gphat/mvn-repo/master/releases/")
}

object Deps {

  val logDeps = List(
    "org.slf4j" % "jcl-over-slf4j" % "1.7.7",
    "org.slf4j" % "slf4j-api" % "1.7.7",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.0.0",
    "ch.qos.logback" % "logback-classic" % "1.1.2" % "runtime",
    "ch.qos.logback" % "logback-core" % "1.1.2" % "runtime")

  val deps = List(
    "org.parboiled"    %% "parboiled" % "2.0.1",
    "com.novus"         %% "salat-core" % "1.9.9",
    "com.novus"         %% "salat-util" % "1.9.9",
    "com.sksamuel.elastic4s" % "elastic4s_2.11" % "1.3.2",
    "org.elasticsearch" % "elasticsearch" % "1.3.2",
    "com.github.scopt" %% "scopt" % "3.2.0",
    "org.scalatra.scalate" % "scalate-core_2.11" % "1.7.0")

  val testingDeps = List(
    "org.scalacheck" %% "scalacheck" % "1.11.5" % "test",
    "org.specs2" %% "specs2" % "2.4.2" % "test",
    "org.elasticsearch" % "elasticsearch" % "1.3.2" % "test",
    "org.scalatest" % "scalatest_2.11" % "2.2.2" % "test")
}
