import sbt._
import Keys._

object LdifGeoBuild extends Build {
  val Organization = "org.noorg"
  val Name = "ldif-geo"
  val Version = "0.1.0-SNAPSHOT"
  val ScalaVersion = "2.10.0"

  val deps = Seq(
    "de.fuberlin.wiwiss.silk" % "silk-singlemachine" % "2.5.4",
    "ldif" % "ldif-silk-local" % "0.5.1",
    "ldif" % "ldif-singlemachine" % "0.5.1",
    "log4j" % "log4j" % "1.2.17",
    "org.apache.jena" % "jena-arq" % "2.11.0",
    "org.apache.jena" % "jena-core" % "2.11.0",
    "org.slf4j" % "jul-to-slf4j" % "1.5.8"
  )

  val mySettings = Seq(
    organization := Organization,
    name := Name,
    version := Version,
    scalaVersion := ScalaVersion,
    resolvers += "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
    resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases",
    resolvers += "External JARs" at "https://github.com/dozed/mvn-repo/raw/master/external",
    resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
    libraryDependencies ++= deps
  )

  lazy val project = Project("ldif-geo", file("."))
    .settings(mySettings:_*)

}