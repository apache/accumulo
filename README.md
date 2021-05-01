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

<!-- LOGO -->
<p align="center">
    <a href="https://vuejs.org" target="_blank" rel="noopener noreferrer">
        <img src="contrib/accumulo-logo.png" alt="Vue logo">
    </a>
</p>

<!-- BADGES -->
<p align="center">
    <!-- Build Status -->
    <a href="https://github.com/apache/accumulo/actions">
        <img src="https://github.com/apache/accumulo/workflows/QA/badge.svg" alt="Build Status">
    </a>
    <!-- Maven Central -->
    <a href="https://maven-badges.herokuapp.com/maven-central/org.apache.accumulo/accumulo-core">
        <img src="https://maven-badges.herokuapp.com/maven-central/org.apache.accumulo/accumulo-core/badge.svg"
            alt="Maven Central">
    </a>
    <!-- JavaDocs -->
    <a href="https://www.javadoc.io/doc/org.apache.accumulo/accumulo-core">
        <img src="https://www.javadoc.io/badge/org.apache.accumulo/accumulo-core.svg" alt="JavaDocs">
    </a>
    <!-- Apache License -->
    <a href="https://www.apache.org/licenses/LICENSE-2.0">
        <img src="https://img.shields.io/badge/license-ASL-blue.svg" alt="Apache License">
    </a>
</p>

# About

[Apache Accumulo®][accumulo] is a sorted, distributed key/value store that provides robust, scalable data storage and retrieval.

With Apache Accumulo, users can store and manage large
data sets across a cluster. Accumulo uses [Apache Hadoop]'s HDFS to store its data and
[Apache Zookeeper] for consensus.

Visit our 🌐 [project website][accumulo] for news and general information

Download the latest version of Apache Accumulo® [here](https://accumulo.apache.org/downloads/).

# Getting Started

Follow the [quick start] to install and run Accumulo

# Documentation

Read the [Accumulo documentation][docs]

# Examples

* Run the [Accumulo examples][examples] to learn how to write Accumulo clients
* View the [Javadocs][javadocs] to learn the [Accumulo API][api]

More resources can be found on the [project website][accumulo].

# Building

Accumulo uses [Maven] to compile, [test], and package its source.

The following
command :

    mvn package

will build the binary `tar.gz` (`assemble/target/accumulo-<version>-bin.tar.gz`) from source.
> _Add `-DskipTests` to build without waiting for the tests to run._

# Contributing

Contributions are welcome to all Apache Accumulo repositories

If you want to contribute, go through our guide [How to contribute](https://accumulo.apache.org/how-to-contribute/)

## Issues

Accumulo uses GitHub [issues](https://github.com/apache/accumulo/issues) to track bugs and new features.

## Pull requests

Please make sure to read the [Contributing Guide](.github/CONTRIBUTING.md) before making a [pull request](https://github.com/apache/accumulo/pulls).

# Code of Conduct

In [Apache Code of Conduct](https://www.apache.org/foundation/policies/conduct.html) you will find the original of [CODE_OF_CONDUCT.md](.github/CODE_OF_CONDUCT.md).

# Export Control

<details>
<summary>Click here to show/hide details</summary>

<br>
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

<br/>

# License

[Apache License, Version 2.0](./LICENSE).

[api]: https://accumulo.apache.org/api
[accumulo]: https://accumulo.apache.org
[quick start]: https://accumulo.apache.org/docs/2.x/getting-started/quickstart
[test]: TESTING.md
[Apache Hadoop]: https://hadoop.apache.org
[Apache Zookeeper]: https://zookeeper.apache.org
[Maven]: https://maven.apache.org
[docs]: https://accumulo.apache.org/latest/accumulo_user_manual
[examples]: https://github.com/apache/accumulo-examples
[javadocs]: https://accumulo.apache.org/latest/apidocs
[java-export]: https://www.oracle.com/us/products/export/export-regulations-345813.html
[bouncy-site]: https://bouncycastle.org

[arch01]: https://lists.apache.org/list.html?user@accumulo.apache.org
[sub01]: user-subscribe@accumulo.apache.org
[unf01]: user-unsubscribe@accumulo.apache.org
[post01]: user@accumulo.apache.org

[arch02]: https://lists.apache.org/list.html?dev@accumulo.apache.org
[sub02]: dev-subscribe@accumulo.apache.org
[unf02]: dev-unsubscribe@accumulo.apache.org
[post02]: dev@accumulo.apache.org

[arch03]: https://lists.apache.org/list.html?commits@accumulo.apache.org
[sub03]: commits-subscribe@accumulo.apache.org
[unf03]: commits-unsubscribe@accumulo.apache.org

[arch04]: https://lists.apache.org/list.html?notifications@accumulo.apache.org
[sub04]: notifications-subscribe@accumulo.apache.org
[unf04]: notifications-unsubscribe@accumulo.apache.org