<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

[![Apache Accumulo][logo]][accumulo]
--
[![Build Status][ti]][tl] [![Maven Central][mi]][ml] [![Javadoc][ji]][jl] [![Apache License][li]][ll]

[Apache Accumulo][accumulo] is a sorted, distributed key/value store that provides robust,
scalable data storage and retrieval. With Apache Accumulo, users can store and manage large
data sets across a cluster. Accumulo uses [Apache Hadoop]'s HDFS to store its data and
[Apache Zookeeper] for consensus.

Download the latest version of Apache Accumulo on the [project website][dl].

## Getting Started

* Follow the [quick start] to install and run Accumulo
* Read the [Accumulo documentation][docs]
* Run the [Accumulo examples][examples] to learn how to write Accumulo clients
* View the [Javadocs][javadocs] to learn the [Accumulo API][api]

More resources can be found on the [project website][accumulo].

## Building

Accumulo uses [Maven] to compile, [test], and package its source. The following
command will build the binary tar.gz from source. Add `-DskipTests` to build without
waiting for the tests to run.

    mvn package

This command produces `assemble/target/accumulo-<version>-bin.tar.gz`

## Contributing

Contributions are welcome to all Apache Accumulo repositories.

If you want to contribute, read [our guide on our website][contribute].

## Export Control

<details>
<summary>Click here to show/hide details</summary>

---

This distribution includes cryptographic software. The country in which you
currently reside may have restrictions on the import, possession, use, and/or
re-export to another country, of encryption software. BEFORE using any
encryption software, please check your country's laws, regulations and
policies concerning the import, possession, or use, and re-export of encryption
software, to see if this is permitted. See <https://www.wassenaar.org/> for more
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
the bouncycastle library for some cryptographic technology as well. See
[the BouncyCastle site][bouncy-site] for
more details on bouncycastle's cryptography features.

</details>

[api]: https://accumulo.apache.org/api
[accumulo]: https://accumulo.apache.org
[logo]: contrib/accumulo-logo.png
[quick start]: https://accumulo.apache.org/docs/2.x/getting-started/quickstart
[test]: TESTING.md
[Apache Hadoop]: https://hadoop.apache.org
[Apache Zookeeper]: https://zookeeper.apache.org
[Maven]: https://maven.apache.org
[docs]: https://accumulo.apache.org/latest/accumulo_user_manual
[examples]: https://github.com/apache/accumulo-examples
[javadocs]: https://accumulo.apache.org/latest/apidocs
[li]: https://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://www.apache.org/licenses/LICENSE-2.0
[mi]: https://maven-badges.herokuapp.com/maven-central/org.apache.accumulo/accumulo-core/badge.svg
[ml]: https://maven-badges.herokuapp.com/maven-central/org.apache.accumulo/accumulo-core/
[ji]: https://www.javadoc.io/badge/org.apache.accumulo/accumulo-core.svg
[jl]: https://www.javadoc.io/doc/org.apache.accumulo/accumulo-core
[ti]: https://github.com/apache/accumulo/workflows/QA/badge.svg
[tl]: https://github.com/apache/accumulo/actions
[java-export]: https://www.oracle.com/us/products/export/export-regulations-345813.html
[bouncy-site]: https://bouncycastle.org
[dl]: https://accumulo.apache.org/downloads
[contribute]: https://accumulo.apache.org/how-to-contribute
