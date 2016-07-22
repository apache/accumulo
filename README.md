<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

Apache Accumulo
===============

The [Apache Accumulo™][1] sorted, distributed key/value store is a robust,
scalable, high performance data storage and retrieval system.  Apache Accumulo
is based on Google's [BigTable][4] design and is built on top of Apache
[Hadoop][5], [Zookeeper][6], and [Thrift][7]. Apache Accumulo features a few
novel improvements on the BigTable design in the form of cell-based access
control and a server-side programming mechanism that can modify key/value pairs
at various points in the data management process. Other notable improvements
and feature are outlined [here][8].

To install and run an Accumulo binary distribution, follow the [install][2]
instructions.
  
Documentation
-------------

Accumulo provides the following documentation :

 * **User Manual** : In-depth developer and administrator documentation.
 * **Examples** : Code with corresponding readme files that give step by step
                  instructions for running example code.

This documentation is available on the [Accumulo site][1].  In the source and
binary distributions of Accumulo, the documentation is at different locations.

In the Accumulo binary distribution, all documentation is in the `docs`
directory.  The binary distribution does not include example source code, but
it does include a jar with the compiled examples.   This examples jar makes it
easy to step through the example readmes, after following the [install][2]
instructions.

In the Accumulo source, documentations is found at the following locations.

 * [Example Source](examples/simple/src/main/java/org/apache/accumulo/examples/simple)
 * [Example Readmes](docs/src/main/resources/examples)
 * [User Manual Source](docs/src/main/asciidoc)

Building 
--------

Accumulo uses [Maven][9] to compile, [test][3], and package its source.  The
following command will build the binary tar.gz from source.  Note, these
instructions will not work for the Accumulo binary distribution as it does not
include source.  If you just want to build without waiting for the tests to
run, add `-DskipTests`.

    mvn package

This command produces a file at the following location.

    assemble/target/accumulo-<version>-bin.tar.gz

API
---

The public Accumulo API is composed of :

All public types in the following packages and their subpackages excluding
those named *impl*, *thrift*, or *crypto*. 

   * org.apache.accumulo.core.client
   * org.apache.accumulo.core.data
   * org.apache.accumulo.core.security
   * org.apache.accumulo.minicluster

A type is a class, interface, or enum.  Anything with public or protected
acccess in an API type is in the API.  This includes, but is not limited to:
methods, members classes, interfaces, and enums.  Package-private types in
the above packages are *not* considered public API.

The following regex matches imports that are *not* Accumulo public API.  This
regex can be used with [RegexpSingleline][13] to automatically find suspicious
imports in a project using Accumulo. 

```
import\s+org\.apache\.accumulo\.(.*\.(impl|thrift|crypto)\..*|(?!core|minicluster).*|core\.(?!client|data|security).*)
```

The Accumulo project maintains binary compatibility across this API within a
major release, as defined in the Java Language Specification 3rd ed. Starting
with Accumulo 1.6.2 and 1.7.0 all API changes will follow [semver 2.0][12]

Export Control
--------------

This distribution includes cryptographic software. The country in which you
currently reside may have restrictions on the import, possession, use, and/or
re-export to another country, of encryption software. BEFORE using any
encryption software, please check your country's laws, regulations and
policies concerning the import, possession, or use, and re-export of encryption
software, to see if this is permitted. See <http://www.wassenaar.org/> for more
information.

The U.S. Government Department of Commerce, Bureau of Industry and Security
(BIS), has classified this software as Export Commodity Control Number (ECCN)
5D002.C.1, which includes information security software using or performing
cryptographic functions with asymmetric algorithms. The form and manner of this
Apache Software Foundation distribution makes it eligible for export under the
License Exception ENC Technology Software Unrestricted (TSU) exception (see the
BIS Export Administration Regulations, Section 740.13) for both object code and
source code.

The following provides more details on the included cryptographic software:

Apache Accumulo uses the built-in java cryptography libraries in its RFile
encryption implementation. See [oracle's export-regulations doc][java-export]
for more details for on Java's cryptography features. Apache Accumulo also uses
the bouncycastle library for some crypographic technology as well. See
[the BouncyCastle FAQ][bouncy-faq] for
more details on bouncycastle's cryptography features.


[1]: http://accumulo.apache.org
[2]: INSTALL.md
[3]: TESTING.md
[4]: http://research.google.com/archive/bigtable.html
[5]: http://hadoop.apache.org
[6]: http://zookeeper.apache.org
[7]: http://thrift.apache.org/
[8]: http://accumulo.apache.org/notable_features.html
[9]: http://maven.apache.org/
[12]: http://semver.org/spec/v2.0.0.html
[13]: http://checkstyle.sourceforge.net/config_regexp.html
[java-export]: http://www.oracle.com/us/products/export/export-regulations-345813.html
[bouncy-faq]: http://www.bouncycastle.org/wiki/display/JA1/Frequently+Asked+Questions
