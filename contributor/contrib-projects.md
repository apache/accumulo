---
title: Contrib Projects
redirect_from: /contrib
---

Apache Accumulo is a complex distributed system. In order to minimize that
complexity for both operators and developers the project maintains contrib
repositories for instructive applications and code that builds interoperability
between Accumulo and other systems. This helps minimize the set of dependencies
needed when building or installing Accumulo, keeps the build time down, and
allows the contrib projects to follow their own release schedule.

## Existing Contrib Projects

Each of the below contrib project handles their own development and release
cycle. For information on what version(s) of Accumulo they work with, see the
documentation for the individual project.

### Instamo Archetype

A Maven Archetype that automates the customization of Instamo to quickly spin
up an Accumulo process in memory.

The Apache Accumulo Instamo Archetype uses [Git][gitbook] version control
([browse][instamo-browse]|[checkout][instamo-checkout]). It builds with [Apache
Maven][maven-proj]. See the section [Contributing to Contrib][contrib2] for
instructions on submitting issues and patches.

### Wikisearch Application

A complex application example that makes use of most of the Accumulo feature
stack. The Wikisearch application provides an example of indexing and querying
Wikipedia data within Accumulo. It is a great place to start if you want to get
familiar with good development practices building on Accumulo. 

For details on setting up the application, see the project&apos;s README. You can
also read [an overview and some performance numbers][wikisearch].

The Apache Accumulo Wikisearch Example uses [Git][gitbook] version control
([browse][wikisearch-browse]|[checkout][wikisearch-checkout]). It builds with
[Apache Maven][maven-proj]. See the section [Contributing to Contrib][contrib2] for
instructions on submitting issues and patches.

### Hama Integration

An implementation for running [Bulk Synchronous Parallel (BSP)
algorithms][bsp-alg] implemented via [Apache Hama][hama] on top of data stored
in Accumulo.

The Apache Accumulo BSP implementation uses [Git][gitbook] version control
([browse][bsp-browse]|[checkout][bsp-checkout]).  It builds with [Apache
Maven][maven-proj]. See the section [Contributing to Contrib][contrib2] for
instructions on submitting issues and patches.

## Contributing to Contrib

All contributions to the various Apache Accumulo contrib projects should follow
the [same process used in the primary project][git-process]. All contributions
should have a corresponding issue filed in the [contrib component in the
Accumulo issue tracker][jira-component].

## Adding a new Contrib Project

Proposals for new contrib projects should be sent to the [Accumulo mailing
list][mailing-list] for [developers][mail-with-subj]. 

If an example application only makes use of a single Accumulo feature, it is
probably better off as an Accumulo version-specific example. You can see
several of these demonstrative applications in the [simple example
codebase][examples-simple] and the related published documentation for versions
[1.7][17EXAMPLES] and [1.6][16EXAMPLES].

[gitbook]: http://git-scm.com
[instamo-browse]: https://git-wip-us.apache.org/repos/asf?p=accumulo-instamo-archetype.git;a=summary
[instamo-checkout]: https://git-wip-us.apache.org/repos/asf/accumulo-instamo-archetype.git
[maven-proj]: https://maven.apache.org
[wikisearch]: example/wikisearch
[wikisearch-browse]: https://git-wip-us.apache.org/repos/asf?p=accumulo-wikisearch.git;a=summary
[wikisearch-checkout]: https://git-wip-us.apache.org/repos/asf/accumulo-wikisearch.git
[bsp-alg]: https://hama.apache.org/hama_bsp_tutorial
[hama]: https://hama.apache.org
[bsp-browse]: https://git-wip-us.apache.org/repos/asf?p=accumulo-bsp.git;a=summary
[bsp-checkout]: https://git-wip-us.apache.org/repos/asf/accumulo-bsp.git
[git-process]: git#the-implementation
[jira-component]: https://issues.apache.org/jira/browse/ACCUMULO/component/12316610
[mailing-list]: mailing_list
[mail-with-subj]: mailto:dev@accumulo.apache.org?subject=[Accumulo+Contrib+Proposal]
[examples-simple]: https://git-wip-us.apache.org/repos/asf?p=accumulo.git;a=tree;f=examples/simple;
[16EXAMPLES]: 1.6/examples
[17EXAMPLES]: 1.7/examples
[contrib2]: #contributing-to-contrib
