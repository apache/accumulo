---
title: Glossary
nav: nav_glossary
---

<dl>
<dt>authorizations</dt>
<dd>a set of strings associated with a user or with a particular scan that will be used to determine which key/value pairs are visible to the user.</dd>
<dt>cell</dt>
<dd>a set of key/value pairs whose keys differ only in timestamp.</dd>
<dt>column</dt>
<dd>the portion of the key that sorts after the row and is divided into family, qualifier, and visibility.</dd>
<dt>column family</dt>
<dd>the portion of the key that sorts second and controls locality groups, the row/column hybrid nature of accumulo.</dd>
<dt>column qualifier</dt>
<dd>the portion of the key that sorts third and provides additional key uniqueness.</dd>
<dt>column visibility</dt>
<dd>the portion of the key that sorts fourth and controls user access to individual key/value pairs. Visibilities are boolean AND (&) and OR (|) combinations of authorization strings with parentheses required to determine ordering, e.g. (AB&C)|DEF.</dd>
<dt>iterator</dt>
<dd>a mechanism for modifying tablet-local portions of the key/value space. Iterators are used for standard administrative tasks as well as for custom processing.</dd>
<dt>iterator priority</dt>
<dd>an iterator must be configured with a particular scope and priority.  When a tablet server enters that scope, it will instantiate iterators in priority order starting from the smallest priority and ending with the largest, and apply each to the data read before rewriting the data or sending the data to the user.</dd>
<dt>iterator scopes</dt>
<dd>the possible scopes for iterators are where the tablet server is already reading and/or writing data: minor compaction / flush time (<em>minc</em> scope), major compaction / file merging time (<em>majc</em> scope), and query time (<em>scan</em> scope).</dd>
<dt>gc</dt>
<dd>process that identifies temporary files in HDFS that are no longer needed by any process, and deletes them.</dd>
<dt>key</dt>
<dd>the key into the distributed sorted map which is accumulo.  The key is subdivided into row, column, and timestamp.  The column is further divided into  family, qualifier, and visibility.</dd>
<dt>locality group</dt>
<dd>a set of column families that will be grouped together on disk.  With no locality groups configured, data is stored on disk in row order.  If each column family were configured to be its own locality group, the data for each column would be stored separately, in row order.  Configuring sets of columns into locality groups is a compromise between the two approaches and will improve performance when multiple columns are accessed in the same scan.</dd>
<dt>log-structured merge-tree</dt>
<dd>the sorting / flushing / merging scheme on which BigTable's design is based.</dd>
<dt>logger</dt>
<dd>in 1.4 and older, process that accepts updates to tablet servers and writes them to local on-disk storage for redundancy. in 1.5 the functionality was subsumed by the tablet server and datanode with HDFS writes.</dd>
<dt>major compaction</dt>
<dd>merging multiple files into a single file.  If all of a tablet's files are merged into a single file, it is called a <em>full major compaction</em>.</dd>
<dt>master</dt>
<dd>process that detects and responds to tablet failures, balances load across tablet servers by assigning and migrating tablets when required, coordinates table operations, and handles tablet server logistics (startup, shutdown, recovery).</dd>
<dt>minor compaction</dt>
<dd>flushing data from memory to disk.  Usually this creates a new file for a tablet, but if the memory flushed is merge-sorted in with data from an existing file (replacing that file), it is called a <em>merging minor compaction</em>.</dd>
<dt>monitor</dt>
<dd>process that displays status and usage information for all Accumulo components.</dd>
<dt>permissions</dt>
<dd>administrative abilities that must be given to a user such as creating tables or users and changing permissions or configuration parameters.</dd>
<dt>row</dt>
<dd>the portion of the key that controls atomicity.  Keys with the same row are guaranteed to remain on a single tablet hosted by a single tablet server, therefore multiple key/value pairs can be added to or removed from a row at the same time. The row is used for the primary sorting of the key.</dd>
<dt>scan</dt>
<dd>reading a range of key/value pairs.</dd>
<dt>tablet</dt>
<dd>a contiguous key range; the unit of work for a tablet server.</dd>
<dt>tablet servers</dt>
<dd>a set of servers that hosts reads and writes for tablets.  Each server hosts a distinct set of tablets at any given time, but the tablets may be hosted by different servers over time.</dd>
<dt>timestamp</dt>
<dd>the portion of the key that controls versioning.  Otherwise identical keys with differing timestamps are considered to be versions of a single <em>cell</em>.  Accumulo can be configured to keep the <em>N</em> newest versions of each <em>cell</em>.  When a deletion entry is inserted, it deletes all earlier versions for its cell.</dd>
<dt>value</dt>
<dd>immutable bytes associated with a particular key.</dd>
</dl>

