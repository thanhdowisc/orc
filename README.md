ORC File Format
===

To build the project, use Maven (3.0.x) from http://maven.apache.org/.

You'll also need to install the protobuf compiler (2.4.x) from https://code.google.com/p/protobuf/.

You'll also need to install the jdo2 jar in your maven repository:
* download [jdo2-api-2.3-ec.jar](http://www.datanucleus.org/downloads/maven2/javax/jdo/jdo2-api/2.3-ec/jdo2-api-2.3-ec.jar) to your working directory
* mvn install:install-file -DgroupId=javax.jdo -DartifactId=jdo2-api -Dversion=2.3-ec -Dpackaging=jar -Dfile=jdo2-api-2.3-ec.jar

Building the jar and running the unit tests:

% mvn package

