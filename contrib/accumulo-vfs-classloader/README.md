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

# VFS Reloading ClassLoader

This module contains a [ClassLoader](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/ClassLoader.html) implementation that can be used as the [System](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/lang/ClassLoader.html#getSystemClassLoader()) ClassLoader.

## Configuration

To use this ClassLoader as the System ClassLoader you must set the JVM system property **java.system.class.loader** to the fully qualified class name (org.apache.accumulo.vfs.ReloadingVFSClassLoader). This jar and it's dependent jars must be on the **java.class.path**. 

To set the classpath for this ClassLoader you must define the variable **vfs.class.loader.classpath** in the environment and set it to locations that are supported by [Apache Commons VFS](http://commons.apache.org/proper/commons-vfs/filesystems.html).

The ClassLoader monitors the classpath for changes at 5 minute intervals. To change this interval define the variable **vfs.classpath.monitor.seconds** in the enviroment.

This ClassLoader follows the normal parent delegation model but can be set to load classes and resources first, before checking if the parent classloader can, by setting the environment variable **vfs.class.loader.delegation** to "post".

Finally, this ClassLoader keeps a local cache of objects pulled from remote systems (via http, etc.). The default location for this cache directory is the value of the system property **java.io.tmpdir**. To change this location set the environment variable **vfs.cache.dir** to an existing directory.

## Implementation

This ClassLoader maintains a [VFSClassLoader](http://commons.apache.org/proper/commons-vfs/commons-vfs2/apidocs/org/apache/commons/vfs2/impl/VFSClassLoader.html) delegate that references the classpath (as specified by **vfs.class.loader.classpath**). The ReloadingVFSClassLoader implements [FileListener](http://commons.apache.org/proper/commons-vfs/commons-vfs2/apidocs/org/apache/commons/vfs2/FileListener.html) and creates a [DefaultFileMonitor](http://commons.apache.org/proper/commons-vfs/commons-vfs2/apidocs/org/apache/commons/vfs2/impl/DefaultFileMonitor.html) that checks for changes on the classpath at the interval specified by **vfs.classpath.monitor.seconds** and creates a new VFSClassLoader delegate. Future requests to load classes and resources will use this new delegate; the old delegate is no longer referenced (except by the classes it has loaded) and can be garbage collected.