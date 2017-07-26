Spring AMQP [<img src="https://build.spring.io/plugins/servlet/wittified/build-status/AMQP-MASTER">](https://build.spring.io/browse/AMQP-MASTER) [![Join the chat at https://gitter.im/spring-projects/spring-amqp](https://badges.gitter.im/spring-projects/spring-amqp.svg)](https://gitter.im/spring-projects/spring-amqp?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
===========

This project provides support for using Spring and Java with [AMQP 0.9.1](https://www.rabbitmq.com/amqp-0-9-1-reference.html), and in particular [RabbitMQ](http://www.rabbitmq.com/).

# Checking out and Building

To check out the project from [GitHub](https://github.com/SpringSource/spring-amqp) and build from source using [Gradle](http://gradle.org/), do the following:

	git clone git://github.com/SpringSource/spring-amqp.git
	cd spring-amqp
	./gradlew build

If you encounter out of memory errors during the build, increase available heap and permgen for Gradle:

	GRADLE_OPTS='-XX:MaxPermSize=1024m -Xmx1024m'

To build and install jars into your local Maven cache:

	./gradlew install

To build api Javadoc (results will be in `build/api`):

	./gradlew api

To build reference documentation (results will be in `build/reference`):

	./gradlew reference

To build complete distribution including `-dist`, `-docs`, and `-schema` zip files (results will be in `build/distributions`)

	./gradlew dist

To analyze and gather metrics using [Sonar](http://www.sonarsource.org/):

	./gradlew clean build sonar

(Please make sure that you have Sonar running, e.g. on localhost port 9000)


# Using Eclipse

To generate Eclipse metadata (.classpath and .project files), do the following:

	./gradlew eclipse

Once complete, you may then import the projects into Eclipse as usual:

*File -> Import -> Existing projects into workspace*

Browse to the *'spring-amqp'* root directory. All projects should import free of errors.

# Using SpringSource Tool Suite™ (STS)

Using the STS Gradle Support, you can directly import Gradle projects, without having to generate Eclipse metadata first (since STS 2.7.M1). Please make sure you have the Gradle STS Extension installed - Please see the [installation  instructions](http://static.springsource.org/sts/docs/latest/reference/html/gradle/installation.html) for details.

1. Select *File -> Import -> Gradle Project*
2. Browse to the Spring AMQP Root Folder
3. Click on **Build Model**
4. Select the projects you want to import
5. Press **Finish**

# Using IntelliJ IDEA

To generate IDEA metadata (.iml and .ipr files), do the following:

    ./gradlew idea

## Distribution Contents

If you downloaded the full Spring AMQP distribution or if you created the distribution using `./gradlew dist`, you will see the following directory structure:

	├── README.md
	├── apache-license.txt
	├── docs
	│	├── api
	│	└── reference
	├── epl-license.txt
	├── libs
	├── notice.txt
	└── schema
	    └── rabbit

The binary JARs and the source code are available in the **libs**. The reference manual and javadocs are located in the **docs** directory.

## Changelog

Lists of issues addressed per release can be found in [JIRA](https://jira.spring.io/browse/AMQP#selectedTab=com.atlassian.jira.plugin.system.project%3Aversions-panel).

## Additional Resources

* [Spring AMQP Homepage](http://www.springsource.org/spring-amqp)
* [Spring AMQP Source](http://github.com/SpringSource/spring-amqp)
* [Spring AMQP Samples](http://github.com/SpringSource/spring-amqp-samples)
* [Spring AMQP Forum](http://forum.springsource.org/forumdisplay.php?f=74)
* [StackOverflow](http://stackoverflow.com/questions/tagged/spring-amqp)

# Contributing to Spring AMQP

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Spring Community Forums.  Please help out on the [forum](http://forum.springsource.org/forumdisplay.php?f=74) by responding to questions and joining the debate.
* Create [JIRA](https://jira.spring.io/browse/AMQP) tickets for bugs and new features and comment and vote on the ones that you are interested in.  
* Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/).  If you want to contribute code this way, please reference a JIRA ticket as well covering the specific issue you are addressing.
* Watch for upcoming articles on Spring by [subscribing](http://www.springsource.org/node/feed) to springframework.org

Before we accept a non-trivial patch or pull request we will need you to sign the [contributor's agreement](https://support.springsource.com/spring_committer_signup).  Signing the contributor's agreement does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do. Active contributors might be asked to join the core team, and given the ability to merge pull requests.

## Code Conventions and Housekeeping
None of these is essential for a pull request, but they will all help.  They can also be added after the original pull request but before a merge.

* Use the Spring Framework code format conventions (import `eclipse-code-formatter.xml` from the root of the project if you are using Eclipse).
* Make sure all new .java files to have a simple Javadoc class comment with at least an @author tag identifying you, and preferably at least a paragraph on what the class is for.
* Add the ASF license header comment to all new .java files (copy from existing files in the project)
* Add yourself as an @author to the .java files that you modify substantially (more than cosmetic changes).
* Add some Javadocs and, if you change the namespace, some XSD doc elements.
* A few unit tests would help a lot as well - someone has to do it.
* If no-one else is using your branch, please rebase it against the current master (or other target branch in the main project).

# License

Spring AMQP is released under the terms of the Apache Software License Version 2.0 (see license.txt).
