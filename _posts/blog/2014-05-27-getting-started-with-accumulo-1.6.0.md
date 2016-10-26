---
title: "Getting Started with Apache Accumulo 1.6.0"
date: 2014-05-27 17:00:00 +0000
author: Josh Elser
---

Originally posted at [https://blogs.apache.org/accumulo/entry/getting_started_with_apache_accumulo](https://blogs.apache.org/accumulo/entry/getting_started_with_apache_accumulo)

On May 12th, 2014, the Apache Accumulo project happily announced version 1.6.0 to the community. This is a new major release for the project which contains many numerous new features and fixes. For the full list of notable changes, I'd recommend that you check out the release notes that were published alongside the release itself. For this post, I'd like to cover some of the changes that have been made at the installation level that are a change for users who are already familiar with the project.

### Download the release

Like always, you can find out releases on the our [downloads page][downloads].  You have the choice of downloading the source and building it yourself, or choosing the binary tarball which already contains pre-built jars for use.

### Native Maps

One of the major components of the original [BigTable][bigtable] design was an "In-Memory Map" which provided fast insert and read operations. Accumulo implements this using a C++ sorted map with a custom allocator which is invoked by the TabletServer using JNI. Each TabletServer uses its own "native" map. It is highly desirable to use this native map as it comes with a notable performance increase over a Java map (which is the fallback when the Accumulo shared library is not found) in addition to greatly reducing the TabletServer's JVM garbage collector stress when ingesting data.

In previous versions, the binary tarball contained a pre-compiled version of the native library (under lib/native/). Shipping a compiled binary was a convenience but also left much confusion when it didn't work on systems which had different, incompatible versions of GCC toolchains installed than what the binary was built against. As such, we have stopped bundling the pre-built shared library in favor of users building this library on their own, and instead include an accumulo-native.tar.gz file within the lib directory which contains the necessary files to build the library yourself.

To reduce the burden on users, we've also introduced a new script inside of the bin directory:

    build_native_map.sh

Invoking this script will automatically unpack, build and install the native map in $ACCUMULO_HOME/lib/native. If you've used older versions of Accumulo, you will also notice that the library name is different in an attempt to better follow standard conventions: libaccumulo.so on Linux and libaccumulo.dylib on Mac OS X.

### Example Configurations

Apache Accumulo still bundles a set of example configuration files in conf/examples. Each sub-directory contains the complete set of files to run on a single node with the named memory limitations. For example, the files contained in conf/examples/3GB/native-standalone will run Accumulo on a single node, with native maps (don't forget to build them first!), within a total memory footprint of 3GB. Copy the contents of one of these directories into conf/ and make sure that your relevant installation details (e.g. HADOOP_PREFIX, JAVA_HOME, etc) are properly set in accumulo-env.sh. For example:

    cp $ACCUMULO_HOME/conf/examples/3G/native-standalone/* $ACCUMULO_HOME/conf

Alternatively, a new script, bootstrap_config.sh, was also introduced that can be invoked instead of manually copying files. It will step through a few choices (memory usage, in-memory map type, and Hadoop major version), and then automatically create the configuration files for you.

    $ACCUMULO_HOME/bin/bootstrap_config.sh

One notable change in these scripts over previous versions is that they default to using Apache Hadoop 2 packaging details, such as the Hadoop conf directory and jar locations. It is highly recommended by the community that you use Apache Accumulo 1.6.0 with at least Apache Hadoop 2.2.0, most notably, to ensure that you will not lose data in the face of power failure. If you are still running on a Hadoop 1 release (1.2.1), you will need to edit both accumulo-env.sh and accumulo-site.xml. There are comments in each file which instruct you what needs to be changed.

### Starting Accumulo

Initializing and starting Accumulo hasn't changed at all! After you have created the configuration files and, if you're using them, built the native maps, run:

    accumulo init

This will prompt you to name your Accumulo instance and set the Accumulo root user's password, then start Accumulo using

    $ACCUMULO_HOME/bin/start-all.sh

[downloads]: http://accumulo.apache.org/downloads/
[bigtable]: http://research.google.com/archive/bigtable.html
