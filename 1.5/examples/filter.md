---
title: Filter Example
---

This is a simple filter example.  It uses the AgeOffFilter that is provided as 
part of the core package org.apache.accumulo.core.iterators.user.  Filters are 
iterators that select desired key/value pairs (or weed out undesired ones).  
Filters extend the org.apache.accumulo.core.iterators.Filter class 
and must implement a method accept(Key k, Value v).  This method returns true 
if the key/value pair are to be delivered and false if they are to be ignored.
Filter takes a "negate" parameter which defaults to false.  If set to true, the
return value of the accept method is negated, so that key/value pairs accepted
by the method are omitted by the Filter.

    username@instance> createtable filtertest
    username@instance filtertest> setiter -t filtertest -scan -p 10 -n myfilter -ageoff
    AgeOffFilter removes entries with timestamps more than <ttl> milliseconds old
    ----------> set AgeOffFilter parameter negate, default false keeps k/v that pass accept method, true rejects k/v that pass accept method: 
    ----------> set AgeOffFilter parameter ttl, time to live (milliseconds): 30000
    ----------> set AgeOffFilter parameter currentTime, if set, use the given value as the absolute time in milliseconds as the current time of day: 
    username@instance filtertest> scan
    username@instance filtertest> insert foo a b c
    username@instance filtertest> scan
    foo a:b []    c
    username@instance filtertest> 
    
... wait 30 seconds ...
    
    username@instance filtertest> scan
    username@instance filtertest> 

Note the absence of the entry inserted more than 30 seconds ago.  Since the
scope was set to "scan", this means the entry is still in Accumulo, but is
being filtered out at query time.  To delete entries from Accumulo based on
the ages of their timestamps, AgeOffFilters should be set up for the "minc"
and "majc" scopes, as well.

To force an ageoff of the persisted data, after setting up the ageoff iterator 
on the "minc" and "majc" scopes you can flush and compact your table. This will
happen automatically as a background operation on any table that is being 
actively written to, but can also be requested in the shell.

The first setiter command used the special -ageoff flag to specify the 
AgeOffFilter, but any Filter can be configured by using the -class flag.  The 
following commands show how to enable the AgeOffFilter for the minc and majc
scopes using the -class flag, then flush and compact the table.

    username@instance filtertest> setiter -t filtertest -minc -majc -p 10 -n myfilter -class org.apache.accumulo.core.iterators.user.AgeOffFilter
    AgeOffFilter removes entries with timestamps more than <ttl> milliseconds old
    ----------> set AgeOffFilter parameter negate, default false keeps k/v that pass accept method, true rejects k/v that pass accept method: 
    ----------> set AgeOffFilter parameter ttl, time to live (milliseconds): 30000
    ----------> set AgeOffFilter parameter currentTime, if set, use the given value as the absolute time in milliseconds as the current time of day: 
    username@instance filtertest> flush
    06 10:42:24,806 [shell.Shell] INFO : Flush of table filtertest initiated...
    username@instance filtertest> compact
    06 10:42:36,781 [shell.Shell] INFO : Compaction of table filtertest started for given range
    username@instance filtertest> flush -t filtertest -w
    06 10:42:52,881 [shell.Shell] INFO : Flush of table filtertest completed.
    username@instance filtertest> compact -t filtertest -w
    06 10:43:00,632 [shell.Shell] INFO : Compacting table ...
    06 10:43:01,307 [shell.Shell] INFO : Compaction of table filtertest completed for given range
    username@instance filtertest>

By default, flush and compact execute in the background, but with the -w flag
they will wait to return until the operation has completed.  Both are 
demonstrated above, though only one call to each would be necessary.  A 
specific table can be specified with -t.

After the compaction runs, the newly created files will not contain any data 
that should have been aged off, and the Accumulo garbage collector will remove 
the old files.

To see the iterator settings for a table, use config.

    username@instance filtertest> config -t filtertest -f iterator
    ---------+---------------------------------------------+---------------------------------------------------------------------------
    SCOPE    | NAME                                        | VALUE
    ---------+---------------------------------------------+---------------------------------------------------------------------------
    table    | table.iterator.majc.myfilter .............. | 10,org.apache.accumulo.core.iterators.user.AgeOffFilter
    table    | table.iterator.majc.myfilter.opt.ttl ...... | 30000
    table    | table.iterator.majc.vers .................. | 20,org.apache.accumulo.core.iterators.user.VersioningIterator
    table    | table.iterator.majc.vers.opt.maxVersions .. | 1
    table    | table.iterator.minc.myfilter .............. | 10,org.apache.accumulo.core.iterators.user.AgeOffFilter
    table    | table.iterator.minc.myfilter.opt.ttl ...... | 30000
    table    | table.iterator.minc.vers .................. | 20,org.apache.accumulo.core.iterators.user.VersioningIterator
    table    | table.iterator.minc.vers.opt.maxVersions .. | 1
    table    | table.iterator.scan.myfilter .............. | 10,org.apache.accumulo.core.iterators.user.AgeOffFilter
    table    | table.iterator.scan.myfilter.opt.ttl ...... | 30000
    table    | table.iterator.scan.vers .................. | 20,org.apache.accumulo.core.iterators.user.VersioningIterator
    table    | table.iterator.scan.vers.opt.maxVersions .. | 1
    ---------+---------------------------------------------+---------------------------------------------------------------------------
    username@instance filtertest> 

When setting new iterators, make sure to order their priority numbers 
(specified with -p) in the order you would like the iterators to be applied.
Also, each iterator must have a unique name and priority within each scope.
