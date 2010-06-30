SPRING AMQP INTEGRATION 1.0.0 M1 (May 12 2010)
-------------------------------

home page: http://www.springsource.org/extensions/se-amqp
forum    : http://forum.springsource.org/forumdisplay.php?f=74

1. INTRODUCTION


2. RELEASE INFO

Release contents:

Spring AMQP is released under the terms of the Apache Software License (see license.txt).

3. DISTRIBUTION JAR FILES

4. WHERE TO START

This distribution contains documentation and two sample applications illustrating the features of Spring AMQP

To build you need the apache qpid libraries. As they are not yet available in a public maven repositories, the solution is to add them manually to your private maven repository. It can be done by executing:

mvn install:install-file -DgroupId=jinterface -DartifactId=jinterface -Dversion=1.4.2 -Dpackaging=maven-plugin -Dfile=OtpErlang.jar

5. ADDITIONAL RESOURCES

