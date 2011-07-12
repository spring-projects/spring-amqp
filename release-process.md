# Build and Release of Spring AMQP

Should be relatively straightforward

> **Requirements:**
>
> * Java 6;
> * Maven 2.2.1
> * (Optional) Ant 1.7.1;
> * If you are on Linux don't use Maven or Ant installed by root (file
>    permission issues with `mvn deploy` - download it and install
>    it as yourself).

## Tagging

Before a release, a tag needs to be created and a final sanity check done.  To do this, a tag is created using Maven and version numbers updated.

To save some time typing version labels, use this (replace version numbers as necessary):

    $ mvn release:prepare -Dtag=X.X.X -DreleaseVersion=X.X.X -DdevelopmentVersion=X.X.X.BUILD-SNAPSHOT -DautoVersionSubmodules=true

You will be asked to confirm the release version and the new development version.  Typically moving from `X.X.X.BUILD-SNAPSHOT` to a named release means the release tag is `X.X.X.RELEASE` and the development version is `X.X.X+1.BUILD-SNAPSHOT`.

## Building and publishing JAR artifacts

In principle the publishing can be done by Maven release plugin:

    $ mvn release:perform

this will deploy to S3 by default.

In practice we often need to tweak stuff and correct errors in the
deployment, and for RELEASE versions we also need to publish to Maven
Central.

To do an M\*, RC\* or RELEASE version you need to be in the tagged copy, so switch to the tag and create a branch

    $ git branch -D release/latest
    $ git checkout <version>
    $ git checkout -b release/latest

### Maven Deploy

Maven can be used to publish the Maven Central artifacts to S3
(e.g. in CI or a quick snapshot release to fix a bug for a user):

    $ mvn -P upload clean deploy

There is a `milestone` profile for publishing a milestone and a
`central` profile for publishing to Maven Central (via Sonatype).

## Building and Publishing ZIP Artifacts

The ZIP artifacts are built by Maven and published via an ant script.
They were already deployed if you followed the steps above (deploy
using the `upload` profile).  Verify this (after site has time to
refresh) by looking for the files in [the community download
page][CommunityDownloads].

To deploy to a local staging for test purposes use this recipe:

    $ mvn -P staging,upload deploy

## Building the Website

    $ export MAVEN_OPTS=-Xmx512m 
    $ mvn site -P samples,release,dist
    $ mvn site:deploy -P samples,dist,release

The last step sends the result to the .org site. Use `mvn site:deploy
-P samples,dist,staging` to test the result before you deploy for real
(it puts it in target/staging).


## Maven Central Deployment

For a full release (not milestones) we also send artifacts to Maven
Central via a pickup directory on Sonatypes OSS repository.  So do
this:

    $ mvn -P central -DskipTests deploy

You need an account with Sonatype and some GPG setup (see
[Sonatype][] for details). The account details need to be added to
your `settings.xml`.

If you make a mistake with the jars, you can re-deploy to the Sonatype
staging area by doing the deploy command again, but once it is
released they are in Maven Central and they never come out again.
