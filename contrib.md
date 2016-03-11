---
title: Contrib Projects
nav: nav_contrib
---

Apache Accumulo is a complex distributed system. In order to minimize that complexity for both operators and developers the project maintains contrib repositories for instructive applications and code that builds interoperability between Accumulo and other systems. This helps minimize the set of dependencies needed when building or installing Accumulo, keeps the build time down, and allows the contrib projects to follow their own release schedule.

## Existing Contrib Projects
Each of the below contrib project handles their own development and release cycle. For information on what version(s) of Accumulo they work with, see the documentation for the individual project.

### Instamo Archetype 
A Maven Archetype that automates the customization of Instamo to quickly spin up an Accumulo process in memory.

The Apache Accumulo Instamo Archetype uses [Git](http://git-scm.com/) version control ([browse](https://git-wip-us.apache.org/repos/asf?p=accumulo-instamo-archetype.git;a=summary)|[checkout](https://git-wip-us.apache.org/repos/asf/accumulo-instamo-archetype.git)). It builds with [Apache Maven](http://maven.apache.org/). See the [section on contributing](#contributing-to-contrib) for instructions on submitting issues and patches.

### Wikisearch Application
A complex application example that makes use of most of the Accumulo feature stack. The Wikisearch application provides an example of indexing and querying Wikipedia data within Accumulo. It is a great place to start if you want to get familiar with good development practices building on Accumulo. 

For details on setting up the application, see the project's README. You can also read [an overview and some performance numbers](example/wikisearch.html).

The Apache Accumulo Wikisearch Example uses [Git](http://git-scm.com/) version control ([browse](https://git-wip-us.apache.org/repos/asf?p=accumulo-wikisearch.git;a=summary)|[checkout](https://git-wip-us.apache.org/repos/asf/accumulo-wikisearch.git)). It builds with [Apache Maven](http://maven.apache.org/). See the [section on contributing](#contributing-to-contrib) for instructions on submitting issues and patches.

### Hama Integration
An implementation for running [Bulk Synchronous Parallel (BSP) algorithms](http://hama.apache.org/hama_bsp_tutorial.html) implemented via [Apache Hama](http://hama.apache.org/) on top of data stored in Accumulo.

The Apache Accumulo BSP implementation uses [Git](http://git-scm.com/) version control ([browse](https://git-wip-us.apache.org/repos/asf?p=accumulo-bsp.git;a=summary)|[checkout](https://git-wip-us.apache.org/repos/asf/accumulo-bsp.git)). It builds with [Apache Maven](http://maven.apache.org/). See the [section on contributing](#contributing-to-contrib) for instructions on submitting issues and patches.

## Contributing to Contrib
All contributions to the various Apache Accumulo contrib projects should follow the [same process used in the primary project](git.html#the-implementation). All contributions should have a corresponding issue filed in the [contrib component in the Accumulo issue tracker](https://issues.apache.org/jira/browse/ACCUMULO/component/12316610).

## Adding a new Contrib Project
Proposals for new contrib projects should be sent to the [Accumulo mailing list](mailing_list.html) for [developers](mailto:dev@accumulo.apache.org?subject=[Accumulo Contrib Proposal]). 

If an example application only makes use of a single Accumulo feature, it is probably better off as an Accumulo version-specific example. You can see several of these demonstrative applications in the [simple example codebase](https://git-wip-us.apache.org/repos/asf?p=accumulo.git;a=tree;f=examples/simple;) and the related published documentation for versions [1.7](1.7/examples/) and [1.6](1.6/examples/).

