Spring AMQP [![Build Status](https://github.com/spring-projects/spring-amqp/actions/workflows/ci-snapshot.yml/badge.svg)](https://github.com/spring-projects/spring-amqp/actions/workflows/ci-snapshot.yml)
[![Revved up by Develocity](https://img.shields.io/badge/Revved%20up%20by-Develocity-06A0CE?logo=Gradle&labelColor=02303A)](https://ge.spring.io/scans?search.rootProjectNames=spring-amqp)
===========

This project provides support for using Spring and Java with [AMQP 0.9.1](https://www.rabbitmq.com/amqp-0-9-1-reference.html), and in particular [RabbitMQ](https://www.rabbitmq.com/).

# Code of Conduct

Please see our [Code of conduct](https://github.com/spring-projects/.github/blob/main/CODE_OF_CONDUCT.md).

# Reporting Security Vulnerabilities

Please see our [Security policy](https://github.com/spring-projects/spring-amqp/security/policy).

# Checking out and Building

To check out the project from [GitHub](https://github.com/SpringSource/spring-amqp) and build from source using [Gradle](https://gradle.org/), do the following:

	git clone git://github.com/SpringSource/spring-amqp.git
	cd spring-amqp
	./gradlew build

If you encounter out of memory errors during the build, increase available heap and permgen for Gradle:

	GRADLE_OPTS='-XX:MaxPermSize=1024m -Xmx1024m'

To build and publish jars to your local Maven repository:

	./gradlew publishToMavenLocal

To build api Javadoc (results will be in `build/api`):

	./gradlew api

To build reference documentation (results will be in `build/site`):

	./gradlew antora

To build complete distribution including `-dist`, `-docs`, and `-schema` zip files (results will be in `build/distributions`)

	./gradlew dist

To analyze and gather metrics using [Sonar](https://www.sonarqube.org/):

	./gradlew clean build sonar

(Please make sure that you have Sonar running, e.g. on localhost port 9000)


# Using Eclipse

To generate Eclipse metadata (.classpath and .project files), do the following:

	./gradlew eclipse

Once complete, you may then import the projects into Eclipse as usual:

*File -> Import -> Existing projects into workspace*

Browse to the *'spring-amqp'* root directory. All projects should import free of errors.

# Using Spring Tools

Using the STS Gradle Support, you can directly import Gradle projects, without having to generate Eclipse metadata first.
Please see the [Spring Tools Home Page](https://spring.io/tools).

1. Select *File -> Import -> Existing Gradle Project*
2. Browse to the Spring AMQP Root Folder
3. Click on **Finish**

# Using IntelliJ IDEA

To generate IDEA metadata (.iml and .ipr files), do the following:

    ./gradlew idea

## Changelog

Lists of issues addressed per release can be found in [Github](https://github.com/spring-projects/spring-amqp/releases).

## Additional Resources

* [Spring AMQP Homepage](https://spring.io/projects/spring-amqp)
* [Spring AMQP Source](https://github.com/SpringSource/spring-amqp)
* [Spring AMQP Samples](https://github.com/SpringSource/spring-amqp-samples)
* [StackOverflow](https://stackoverflow.com/questions/tagged/spring-amqp)

# Contributing to Spring AMQP

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on Stack Overflow by responding to questions and joining the debate.

* Create Github issues for bugs and new features and comment and vote on the ones that you are interested in.
* Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/fork-a-repo).
If you want to contribute code this way, please reference the specific Github issue you are addressing.

Before we accept a non-trivial patch or pull request we will need you to sign the [contributor's agreement](https://cla.pivotal.io/sign/spring).
Signing the contributor's agreement does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do.
Active contributors might be asked to join the core team, and given the ability to merge pull requests.

## Code Conventions and Housekeeping
None of these is essential for a pull request, but they will all help.  They can also be added after the original pull request but before a merge.

* Use the Spring Framework code format conventions (import `eclipse-code-formatter.xml` from the root of the project if you are using Eclipse).
* Make sure all new .java files to have a simple Javadoc class comment with at least an @author tag identifying you, and preferably at least a paragraph on what the class is for.
* Add the ASF license header comment to all new .java files (copy from existing files in the project)
* Add yourself as an @author to the .java files that you modify substantially (more than cosmetic changes).
* Add some Javadocs and, if you change the namespace, some XSD doc elements.
* A few unit tests would help a lot as well - someone has to do it.
* If no-one else is using your branch, please rebase it against the current main (or other target branch in the main project).

# License

Spring AMQP is released under the terms of the Apache Software License Version 2.0 (see LICENSE.txt).
