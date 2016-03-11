---
title: Verifying a Release
nav: nav_verify_release
---

This is a guide for the verification of a release candidate of Apache Accumulo. These steps are meant to encapsulate
the requirements of the PMC set forth by the Foundation itself.

The information here is meant to be an application of Foundation policy. When in doubt or conflict, any Foundation-level
trumps anything written here.

Verification of a release candidate can be broken down into three categories.

## Accumulo Correctness ##

Testing a distributed database is, arguably, the easiest of these requirements.

Accumulo contains unit and integration tests which can be automatically run via Maven. These tests can be invoked
by issues the following commands:

    $ mvn verify

Additionally, Accumulo contains multiple distributed tests, most notably the RandomWalk and ContinuousIngest tests.
Information on these tests can be found in their respective directories, `test/system/randomwalk` and
 `test/system/continuous`, which include instructions on how to run the tests. These tests are intended to be run
for days on end while injecting faults into the system. These are the tests that truly verify the correctness of
Accumulo on real systems.

More information on these tests, and the requirements in running them for a given release, can be found on the
[governance page on releasing][1]

## Foundation Level Requirements ##

The ASF requires that all artifacts in a release are cryptographically signed and distributed with hashes.

OpenPGP is an asymmetric encryption scheme which lends itself well to the globally distributed nature of Apache.
Verification of a release artifact can be done using the signature and the release-maker's public key. Hashes
can be verified using the appropriate command (e.g. `sha1sum`, `md5sum`).

An Apache release must contain a source-only artifact. This is the official release artifact. While a release of
an Apache project can contain other artifacts that do contain binary files. These non-source artifacts are for
user convenience only, but still must adhere to the same licensing rules.

PMC members should take steps to verify that the source-only artifact does not contain any binary files. There is
some leeway in this rule. For example, test-only binary artifacts (such as test files or jars) are acceptable as long
as they are only used for testing the software and not running it.

The following are the aforementioned Foundation-level documents provided for reference:

* [Applying the Apache Software License][2]
* [Legal's license application guidelines][3]
* [Common legal-discuss mailing list questions/resolutions][4]
* [ASF Legal Affairs Page][5]

## Apache Software License Application ##

Application of the Apache Software License v2 consists of the following steps on each artifact in a release. It's
important to remember that for artifacts that contain other artifacts (e.g. a tarball that contains JAR files or
an RPM which contains JAR files), both the tarball, RPM and JAR files are subject to the following roles.

The difficulty in verifying each artifact is that, often times, each artifact requires a different LICENSE and NOTICE
file. For example, the Accumulo binary tarball must contain appropriate LICENSE and NOTICE files considering the bundled
jar files in `lib/`. The Accumulo source tarball would not contain these same contents in the LICENSE and NOTICE files
as it does not contain those same JARs.

### LICENSE file ###

The LICENSE file should be present at the top-level of the artifact. This file should be explicitly named `LICENSE`,
however `LICENSE.txt` is acceptable but not preferred. This file contains the text of the Apache Software License 
at the top of the file. At the bottom of the file, all other open source licenses _contained in the given
artifact_ must be listed at the bottom of the LICENSE file. Contained components that are licensed with the ASL themselves
do not need to be included in this file. It is common to see inclusions in file such as the MIT License of 3-clause
BSD License.

### NOTICE file ###

The NOTICE file should be present at the top-level of the artifact beside the LICENSE file. This file should be explicitly
name `NOTICE`, while `NOTICE.txt` is also acceptable but not preferred. This file contains the copyright notice for
the artifact being released. As a reminder, the copyright is held by the Apache Software Foundation, not the individual
project.

The second purpose this file serves is to distribute third-party notices from dependent software. Specifically, other code
which is licensed with the ASLv2 may also contain a NOTICE file. If such an artifact which contains a NOTICE file is
contained in artifact being verified for releases, the contents of the contained artifact's NOTICE file should be appended
to this artifact's NOTICE file. For example, Accumulo bundles the Apache Thrift libthrift JAR file which also have its
own NOTICE file. The contents of the Apache Thrift NOTICE file should be included within Accumulo's NOTICE file.

[1]: {{ site.baseurl }}/governance/releasing.html#testing
[2]: http://www.apache.org/dev/apply-license.html
[3]: http://www.apache.org/legal/src-headers.html
[4]: http://apache.org/legal/resolved.html
[5]: http://www.apache.org/legal/
