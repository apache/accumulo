---
title: Filter Example
---

This is a simple filter example.  It uses the AgeOffFilter that is provided as 
part of the core package org.apache.accumulo.core.iterators.filter.  Filters are used by
the FilteringIterator to select desired key/value pairs (or weed out undesired 
ones).  Filters implement the org.apache.accumulo.core.iterators.iterators.filter.Filter interface which 
contains a method accept(Key k, Value v).  This method returns true if the key, 
value pair are to be delivered and false if they are to be ignored.

    username@instance> createtable filtertest
    username@instance filtertest> setiter -t filtertest -scan -p 10 -n myfilter -filter
    FilteringIterator uses Filters to accept or reject key/value pairs
    ----------> entering options: <filterPriorityNumber> <ageoff|regex|filterClass>
    ----------> set org.apache.accumulo.core.iterators.FilteringIterator option (<name> <value>, hit enter to skip): 0 ageoff
    ----------> set org.apache.accumulo.core.iterators.FilteringIterator option (<name> <value>, hit enter to skip): 
    AgeOffFilter removes entries with timestamps more than <ttl> milliseconds old
    ----------> set org.apache.accumulo.core.iterators.filter.AgeOffFilter parameter currentTime, if set, use the given value as the absolute time in milliseconds as the current time of day: 
    ----------> set org.apache.accumulo.core.iterators.filter.AgeOffFilter parameter ttl, time to live (milliseconds): 30000
    username@instance filtertest> 
    
    username@instance filtertest> scan
    username@instance filtertest> insert foo a b c
    username@instance filtertest> scan
    foo a:b []    c
    
... wait 30 seconds ...
    
    username@instance filtertest> scan
    username@instance filtertest>

Note the absence of the entry inserted more than 30 seconds ago.  Since the
scope was set to "scan", this means the entry is still in Accumulo, but is
being filtered out at query time.  To delete entries from Accumulo based on
the ages of their timestamps, AgeOffFilters should be set up for the "minc"
and "majc" scopes, as well.

To force an ageoff in the persisted data, after setting up the ageoff iterator 
on the "minc" and "majc" scopes you can flush and compact your table. This will
happen automatically as a background operation on any table that is being 
actively written to, but these are the commands to force compaction:

    username@instance filtertest> setiter -t filtertest -scan -minc -majc -p 10 -n myfilter -filter
    FilteringIterator uses Filters to accept or reject key/value pairs
    ----------> entering options: <filterPriorityNumber> <ageoff|regex|filterClass>
    ----------> set org.apache.accumulo.core.iterators.FilteringIterator option (<name> <value>, hit enter to skip): 0 ageoff
    ----------> set org.apache.accumulo.core.iterators.FilteringIterator option (<name> <value>, hit enter to skip): 
    AgeOffFilter removes entries with timestamps more than <ttl> milliseconds old
    ----------> set org.apache.accumulo.core.iterators.filter.AgeOffFilter parameter currentTime, if set, use the given value as the absolute time in milliseconds as the current time of day: 
    ----------> set org.apache.accumulo.core.iterators.filter.AgeOffFilter parameter ttl, time to live (milliseconds): 30000
    username@instance filtertest> 
    
    username@instance filtertest> flush -t filtertest
    08 11:13:55,745 [shell.Shell] INFO : Flush of table filtertest initiated...
    username@instance filtertest> compact -t filtertest
    08 11:14:10,800 [shell.Shell] INFO : Compaction of table filtertest scheduled for 20110208111410EST
    username@instance filtertest> 

After the compaction runs, the newly created files will not contain any data that should be aged off, and the
Accumulo garbage collector will remove the old files.

To see the iterator settings for a table, use:

    username@instance filtertest> config -t filtertest -f iterator
    ---------+------------------------------------------+----------------------------------------------------------
    SCOPE    | NAME                                     | VALUE
    ---------+------------------------------------------+----------------------------------------------------------
    table    | table.iterator.majc.myfilter .............. | 10,org.apache.accumulo.core.iterators.FilteringIterator
    table    | table.iterator.majc.myfilter.opt.0 ........ | org.apache.accumulo.core.iterators.filter.AgeOffFilter
    table    | table.iterator.majc.myfilter.opt.0.ttl .... | 30000
    table    | table.iterator.majc.vers .................. | 20,org.apache.accumulo.core.iterators.VersioningIterator
    table    | table.iterator.majc.vers.opt.maxVersions .. | 1
    table    | table.iterator.minc.myfilter .............. | 10,org.apache.accumulo.core.iterators.FilteringIterator
    table    | table.iterator.minc.myfilter.opt.0 ........ | org.apache.accumulo.core.iterators.filter.AgeOffFilter
    table    | table.iterator.minc.myfilter.opt.0.ttl .... | 30000
    table    | table.iterator.minc.vers .................. | 20,org.apache.accumulo.core.iterators.VersioningIterator
    table    | table.iterator.minc.vers.opt.maxVersions .. | 1
    table    | table.iterator.scan.myfilter .............. | 10,org.apache.accumulo.core.iterators.FilteringIterator
    table    | table.iterator.scan.myfilter.opt.0 ........ | org.apache.accumulo.core.iterators.filter.AgeOffFilter
    table    | table.iterator.scan.myfilter.opt.0.ttl .... | 30000
    table    | table.iterator.scan.vers .................. | 20,org.apache.accumulo.core.iterators.VersioningIterator
    table    | table.iterator.scan.vers.opt.maxVersions .. | 1
    ---------+------------------------------------------+----------------------------------------------------------
    username@instance filtertest> 

If you would like to apply multiple filters, this can be done using a single
iterator. Just continue adding entries during the 
"set org.apache.accumulo.core.iterators.FilteringIterator option" step.
Make sure to order the filterPriorityNumbers in the order you would like
the filters to be applied.
