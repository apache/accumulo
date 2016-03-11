---
title: Source Code and Developers Guide
skiph1fortitle: true
nav: nav_source
---

<div class="panel panel-default pull-right">
<div class="panel-heading">Quick Links</div>
<div class="list-group">
<a href="https://git-wip-us.apache.org/repos/asf?p=accumulo.git;a=summary" class="list-group-item"><i class="fa fa-external-link"></i> Accumulo source</a>
<a href="https://builds.apache.org/job/Accumulo-Master/" class="list-group-item"><i class="fa fa-external-link"></i> Master build on Jenkins</a>
<a href="https://issues.apache.org/jira/browse/accumulo" class="list-group-item"><i class="fa fa-external-link"></i> Accumulo JIRA</a>
</div>
</div>

## Source Code

### Apache Accumulo

Apache Accumulo&trade; source code is maintained using [Git][git] version control 
([browse][cgit]|[checkout][anongit]).  It builds with [Apache Maven][maven].

Instructions for configuring git are [here][git-instr].

### Contrib Projects

Accumulo has a number of [contrib projects][contrib] that maintain their own code repositories and release schedules.

### Website

Accumulo's web site is maintained with [Apache Subversion][subversion] [here][sitesvn] using Apache's [Content Management System][cms].
Committers may edit the site by following [these instructions][cmsusage].  Non-committers should follow
[this FAQ entry][cmsanon].

## Developer's Guide

### Building

#### Installing Thrift

If you activate the 'thrift' Maven profile, the build of some modules will attempt to run the Apache Thrift command line to regenerate
stubs. If you activate this profile and don't have Apache Thrift installed and in your path, you will see a warning and
your build will fail. For Accumulo 1.5.0 and greater, install Thrift 0.9 and make sure that the 'thrift' command is in your path. 
Watch out for THRIFT-1367; you may need to configure Thrift with --without-ruby. Most developers do not
need to install or modify the Thrift definitions as a part of developing against Apache Accumulo.

#### Checking out from Git

To check out the code:

    git clone https://git-wip-us.apache.org/repos/asf/accumulo.git

#### Running a Build

Accumulo uses  [Apache Maven][maven] to handle source building, testing, and packaging. To build Accumulo you will need to use Maven version 3.0.4 or later.

You should familiarize yourself with the [Maven Build Lifecycle][lifecycle], as well as the various plugins we use in our [POM][pom], in order to understand how Maven works and how to use the various build options while building Accumulo.

To build from source (for example, to deploy):

    mvn package -Passemble

This will create a file accumulo-*-SNAPSHOT-dist.tar.gz in the assemble/target directory. Optionally, append `-DskipTests` if you want to skip the build tests.

To build your branch before submitting a pull request, you'll probably want to run some basic "sunny-day" integration tests to ensure you haven't made any grave errors, as well as `checkstyle` and `findbugs`:

    mvn verify -Psunny

To run specific unit tests, you can run:

    mvn package -Dtest=MyTest -DfailIfNoTests=false

Or to run the specific integration tests MyIT and YourIT (and skip all unit tests), you can run:

    mvn package -Dtest=NoSuchTestExists -Dit.test=MyIT,YourIT -DfailIfNoTests=false

There are plenty of other options. For example, you can skip findbugs with `mvn verify -Dfindbugs.skip` or checkstyle `-Dcheckstyle.skip`, or control the number of forks to use while executing tests, `-DforkCount=4`, etc. You should check with specific plugins to see which command-line options are available to control their behavior. Note that not all options will result in a stable build, and options may change over time.

If you regularly switch between major development branches, you may receive errors about improperly licensed files from the [RAT plugin][1]. This is caused by modules that exist in one branch and not the other leaving Maven build files that the RAT plugin no longer understands how to ignore.

The easiest fix is to ensure all of your current changes are stored in git and then cleaning your workspace.

    $> git add path/to/file/that/has/changed
    $> git add path/to/other/file
    $> git clean -df

Note that this git clean command will delete any files unknown to git in a way that is irreversible. You should check that no important files will be included by first looking at the "untracked files" section in a ```git status``` command.

    $> git status
    # On branch master
    nothing to commit (working directory clean)
    $> mvn package
    { maven output elided }
    $> git checkout 1.6.1-SNAPSHOT
    Switched to branch '1.6.1-SNAPSHOT'
    $> git status
    # On branch 1.6.1-SNAPSHOT
    # Untracked files:
    #   (use "git add <file>..." to include in what will be committed)
    #
    #	mapreduce/
    #	shell/
    nothing added to commit but untracked files present (use "git add" to track)
    $> git clean -df
    Removing mapreduce/
    Removing shell/
    $> git status
    # On branch 1.6.1-SNAPSHOT
    nothing to commit (working directory clean)

### Continuous Integration

Accumulo uses [Jenkins][jenkins] for automatic builds.

<img src="https://builds.apache.org/job/Accumulo-Master/lastBuild/buildStatus" style="height: 1.1em"> [Master][masterbuild]

<img src="https://builds.apache.org/job/Accumulo-1.7/lastBuild/buildStatus" style="height: 1.1em"> [1.7 Branch][17build]

<img src="https://builds.apache.org/job/Accumulo-1.6/lastBuild/buildStatus" style="height: 1.1em"> [1.6 Branch][16build]

### Issue Tracking

Accumulo [tracks issues][jiraloc] with [JIRA][jira].  Every commit should reference a JIRA ticket of the form ACCUMULO-#.

### Merging Practices

Changes should be merged from earlier branches of Accumulo to later branches.  Ask the [dev list][devlist] for instructions.

### Public API

Refer to the README in the release you are using to see what packages are in the public API.

Changes to non-private members of those classes are subject to additional scrutiny to minimize compatibility problems across Accumulo versions.

### Coding Practices

<table class="table">
<tr><th>License Header</th><td>Always add the current ASF license header as described in <a href="http://www.apache.org/legal/src-headers.html">ASF Source Header</a>.</td></tr>
<tr><th>Trailing Whitespaces</th><td>Remove all trailing whitespaces. Eclipse users can use Source&rarr;Cleanup option to accomplish this.</td></tr>
<tr><th>Indentation</th><td>Use 2 space indents and never use tabs!</td></tr>
<tr><th>Line Wrapping</th><td>Use 160-column line width for Java code and Javadoc.</td></tr>
<tr><th>Control Structure New Lines</th><td>Use a new line with single statement if/else blocks.</td></tr>
<tr><th>Author Tags</th><td>Do not use Author Tags. The code is developed and owned by the community.</td></tr>
</table>

### Code Review

Accumulo has [guidelines for using Review Board][rb] to support code reviews.

### IDE Configuration Tips

#### Eclipse

* Download Eclipse [formatting and style guides for Accumulo][styles].
* Import Formatter: Preferences > Java > Code Style >  Formatter and import the Eclipse-Accumulo-Codestyle.xml downloaded in the previous step. 
* Import Template: Preferences > Java > Code Style > Code Templates and import the Eclipse-Accumulo-Template.xml. Make sure to check the "Automatically add comments" box. This template adds the ASF header and so on for new code.

#### IntelliJ

 * Formatter [plugin][intellij-formatter] that uses eclipse code style xml.

### Release Guide

Accumulo's release guide can be found [here][release].

[subversion]: http://subversion.apache.org/
[sitesvn]: https://svn.apache.org/repos/asf/accumulo/site/
[maven]: http://maven.apache.org/
[srcheaders]: http://www.apache.org/legal/src-headers.html
[styles]: https://git-wip-us.apache.org/repos/asf?p=accumulo.git;a=tree;f=contrib;hb=HEAD
[jenkins]: http://jenkins-ci.org/
[masterbuild]: https://builds.apache.org/job/Accumulo-Master/
[17build]: https://builds.apache.org/job/Accumulo-1.7/
[16build]: https://builds.apache.org/job/Accumulo-1.6/
[jiraloc]: https://issues.apache.org/jira/browse/accumulo
[jira]: http://www.atlassian.com/software/jira/overview
[devlist]: mailto:dev@accumulo.apache.org
[release]: governance/releasing.html
[cms]: http://www.apache.org/dev/cms.html
[cmsusage]: http://www.apache.org/dev/cms.html#usage
[cmsanon]: http://www.apache.org/dev/cmsref.html#non-committer
[git]: http://git-scm.com/
[cgit]: https://git-wip-us.apache.org/repos/asf?p=accumulo.git;a=summary
[anongit]: git://git.apache.org/accumulo.git
[rb]: rb.html
[pom]: https://git-wip-us.apache.org/repos/asf?p=accumulo.git;a=blob_plain;f=pom.xml;hb=HEAD
[lifecycle]: https://maven.apache.org/guides/introduction/introduction-to-the-lifecycle.html
[1]: http://creadur.apache.org/rat/apache-rat-plugin/
[git-instr]: https://git-wip-us.apache.org
[intellij-formatter]: https://code.google.com/p/eclipse-code-formatter-intellij-plugin
[contrib]: contrib.html
