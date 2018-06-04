import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
                  organization := "io.redpencil",
                  scalaVersion := "2.12.3",
                  version      := "0.1.0-SNAPSHOT"
                )),
    name := "ttl-importer",
    libraryDependencies ++= Seq(
      scalaTest % Test,
      "org.eclipse.rdf4j" % "rdf4j-rio-rdfxml" % rdf4jVersion,
      "org.eclipse.rdf4j" % "rdf4j-rio-turtle" % rdf4jVersion,
      "org.eclipse.rdf4j" % "rdf4j-repository-sparql" % rdf4jVersion,
      "commons-logging" % "commons-logging" % "1.2"
    )
  )
