name := "Distributed Matrix Multiplication"

version := "1.0"

  
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.0.2",
		            "org.scalanlp" %% "breeze" % "0.9",
		            "org.apache.hadoop" % "hadoop-client" % "1.2.1",
			        "org.scalanlp" %% "breeze-natives" % "0.9",
	                    "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)


