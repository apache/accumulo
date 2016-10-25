---
title: "The Accumulo ClassLoader"
date: 2014-05-03 17:00:00 +0000
author: Dave Marion
---

Originally posted at [https://blogs.apache.org/accumulo/](https://blogs.apache.org/accumulo/)

The Accumulo classloader is an integral part of the software. The classloader is created before each of the services (master, tserver, gc, etc) are started and it is set as the classloader for that service. The classloader was rewritten in version 1.5 and this article will explain the new behavior.

### First, some history

The classloader in version 1.4 used a simple hierarchy of two classloaders that would load classes from locations specified by two properties. The locations specified by the "general.classpaths" property would be used to create a parent classloader and locations specified by the "general.dynamic.classpaths" property were used to create a child classloader. The child classloader would monitor the specified locations for changes and when a change occurred the child classloader would be replaced with a new instance. Classes that referenced the orphaned child classloader would continue to work and the classloader would be garbage collected when no longer referenced. The diagram below shows the relationship between the classloaders in Accumulo 1.4.

The only place where the dynamic classloader would come into play is for user iterators and their dependencies. The general advice for using this classloader would be to put the jars containing your iterators in the dynamic location. Everything else that does not change very often or would require a restart should be put into the non-dynamic location.

There are a couple of things to note about the classloader in 1.4. First, if you modified the dynamic locations too often, you would run out of perm-gen space. This is likely due to unreferenced classes not being unloaded from the JVM. This is captured in [ACCUMULO-599]. Secondly, when you modified files in dynamic locations within the same cycle, it would on occasion miss the second change.

### Out with the old, in with the new

The Accumulo classloader was rewritten in version 1.5. It maintains the same dynamic capability and includes a couple of new features. The classloader uses [Commons VFS][commonsvfs] so that it can load jars and classes from a variety of sources, including HDFS. Being able to load jars from one location (hdfs, http, etc) will make it easier to deploy changes to your cluster. Additionally, we introduced the notion of classloader contexts into Accumulo. This is not a new concept for anyone that has used an application server, but the implementation is a little different for Accumulo.

The hierarchy set up by the new classloader uses the same property names as the old classloader. In the most basic configuration the locations specified by "general.classpaths" are used to create the root of the application classloader hierarchy. This classloader is a [URLClassLoader] and it does not support dynamic reloading. If you only specify this property, then you are loading all of your jars from the local file system and they will not be monitored for changes. We will call this top level application classloader the SYSTEM classloader. Next, a classloader is created that supports VFS sources and reloading. The parent of this classloader is the SYSTEM classloader and we will call this the VFS classloader. If the "general.vfs.classpaths" property is set, the VFS classloader will use this location. If the property is not set, it will use the value of "general.dynamic.classpaths" with a default value of $ACCUMULO_HOME/lib/ext to support backwards compatibility. The diagram below shows the relationship between the classloaders in Accumulo 1.5.

### Running Accumulo From HDFS

If you have defined "general.vfs.classpaths" in your Accumulo configuration, then you can use the bootstrap_hdfs.sh script in the bin directory to seed HDFS with the Accumulo jars. A couple of jars will remain on the local file system for starting services. Now when you start up Accumulo the master, gc, tracer, and all of the tablet servers will get their jars and classes from HDFS. The bootstrap_hdfs.sh script sets the replication on the directory, but you may want to set it higher after bootstrapping. An example configuration setting would be:

```xml
<property>
  <name>general.vfs.classpaths</name>
  <value>hdfs://localhost:8020/accumulo/system-classpath</value>
  <description>Configuration for a system level vfs classloader. Accumulo jars can be configured here and loaded out of HDFS.</description>
</property>
```

### About Contexts

You can also define classloader contexts in your accumulo-site.xml file. A context is defined by a user supplied name and it references locations like the other classloader properties. When a context is defined in the configuration, it can then be applied to one or more tables. When a context is applied to a table, then a classloader is created for that context. If multiple tables use the same context, then they share the context classloader. The context classloader is a child to the VFS classloader created above.

The goal here is to enable multiple tenants to share the same Accumulo instance. For example, we may have a context called 'app1' which references the jars for application A. We may also have another context called app2 which references the jars for application B. By default the context classloader delegates to the VFS classloader. This behavior may be overridden as seen in the app2 example below. The context classloader also supports reloading like the VFS classloader.

```xml
<property>
  <name>general.vfs.context.classpath.app1</name>
  <value>hdfs://localhost:8020/applicationA/classpath/.*.jar,file:///opt/applicationA/lib/.*.jar</value>
  <description>Application A classpath, loads jars from HDFS and local file system</description>
</property>

<property>
  <name>general.vfs.context.classpath.app2.delegation=post</name>
  <value>hdfs://localhost:8020/applicationB/classpath/.*.jar,http://my-webserver/applicationB/.*.jar</value>
  <description>Application B classpath, loads jars from HDFS and HTTP, does not delegate to parent first</description>
</property>
```

Context classloaders do not have to be defined in the accumulo-site.xml file. The "general.vfs.context.classpath.{context}" property can be defined on the table either programatically or manually in the shell. Then set the "table.classpath.context" property on your table.

### Known Issues

Remember the two issues I mentioned above? Well, they are still a problem.

* [ACCUMULO-1507] is tracking [VFS-487] for frequent modifications to files.
* If you start running out of perm-gen space, take a look at [ACCUMULO-599] and try applying the JVM settings for class unloading.
* Additionally, there is an issue with the bootstrap_hdfs.sh script detailed in [ACCUMULO-2761]. There is a workaround listed in the issue.

Please email the [dev](mailto:dev@accumulo.apache.org) list for comments and questions.

[ACCUMULO-1507]: https://issues.apache.org/jira/browse/ACCUMULO-1507
[ACCUMULO-599]: https://issues.apache.org/jira/browse/ACCUMULO-599
[ACCUMULO-2761]: https://issues.apache.org/jira/browse/ACCUMULO-2761
[VFS-487]: https://issues.apache.org/jira/browse/VFS-487
[commonsvfs]: http://commons.apache.org/proper/commons-vfs/
[URLClassLoader]: http://docs.oracle.com/javase/6/docs/api/java/net/URLClassLoader.html
