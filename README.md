This project provides support for using Spring and Java with AMQP, and in particular RabbitMQ.

# Build Instructions #

Clone from GIT and then use Maven (2.1.*):

    $ git clone ...
    $ mvn install

SpringSource ToolSuite users (or Eclipse users with the latest m2eclipse plugin) can import the projects as existing Eclipse projects.

# Contributing to Spring AMQP

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Spring Community Forums.  Please help out on the [forum](http://forum.springsource.org/forumdisplay.php?f=74) by responding to questions and joining the debate.
* Create [JIRA](https://jira.springsource.org/browse/AMQP) tickets for bugs and new features and comment and vote on the ones that you are interested in.  
* Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/).  If you want to contribute code this way, please reference a JIRA ticket as well covering the specific issue you are addressing.
* Watch for upcoming articles on Spring by [subscribing](http://www.springsource.org/node/feed) to springframework.org

Before we accept a non-trivial patch or pull request we will need you to sign the [contributor's agreement](https://support.springsource.com/spring_committer_signup).  Signing the contributor's agreement does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do.  Active contributors might be asked to join the core team, and given the ability to merge pull requests.

## Code Conventions and Housekeeping
None of these is essential for a pull request, but they will all help.  They can also be added after the original pull request but before a merge.
* Use the Spring Framework code format conventions (import `eclipse-code-formatter.xml` from the root of the project if you are using Eclipse).
* Make sure all new .java files to have a simple Javadoc class comment with at least an @author tag identifying you, and preferably at least a paragraph on what the class is for.
* Add the ASF license header comment to all new .java files (copy from existing files in the project)
* Add yourself as an @author to the .java files that you modify substantially (moew than cosmetic changes).
* Add some Javadocs and, if you change the namespace, some XSD doc elements (Spring AMQP is not stellar in this area yet, but it will have to come up to scratch one day, so you will be helping a lot).
* A few unit tests would help a lot as well - someone has to do it.
