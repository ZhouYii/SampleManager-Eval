name := "graph-sampling"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"    % "1.3.1",
  "org.apache.spark" %% "spark-mllib"    % "1.2.0",
  "org.apache.spark" %% "spark-graphx"  % "1.3.0")

resolvers += Resolver.sonatypeRepo("snapshots")

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "edu.berkeley.cs.amplab" %% "spark-indexedrdd" % "0.1-SNAPSHOT"
