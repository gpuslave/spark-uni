name := "App"

version := "1"

scalaVersion := "2.12.20"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.5"

// 1. Declare the main class to embed in the manifest
assembly / mainClass := Some("App")

// 2. (Optional) Customize the output JAR name
assembly / assemblyJarName := "app-assembly.jar"

// 3. Handle META‑INF merge conflicts
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _                             => MergeStrategy.first
}