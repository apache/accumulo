---
title: Combiner Example
---

This tutorial uses the following Java class, which can be found in org.apache.accumulo.examples.simple.combiner in the simple-examples module:

 * StatsCombiner.java - a combiner that calculates max, min, sum, and count

This is a simple combiner example.  To build this example run maven and then
copy the produced jar into the accumulo lib dir.  This is already done in the
tar distribution.

    $ bin/accumulo shell -u username
    Enter current password for 'username'@'instance': ***
    
    Shell - Apache Accumulo Interactive Shell
    - 
    - version: 1.4.x
    - instance name: instance
    - instance id: 00000000-0000-0000-0000-000000000000
    - 
    - type 'help' for a list of available commands
    - 
    username@instance> createtable runners
    username@instance runners> setiter -t runners -p 10 -scan -minc -majc -n decStats -class org.apache.accumulo.examples.simple.combiner.StatsCombiner
    Combiner that keeps track of min, max, sum, and count
    ----------> set StatsCombiner parameter all, set to true to apply Combiner to every column, otherwise leave blank. if true, columns option will be ignored.: 
    ----------> set StatsCombiner parameter columns, <col fam>[:<col qual>]{,<col fam>[:<col qual>]} escape non aplhanum chars using %<hex>.: stat
    ----------> set StatsCombiner parameter radix, radix/base of the numbers: 10
    username@instance runners> setiter -t runners -p 11 -scan -minc -majc -n hexStats -class org.apache.accumulo.examples.simple.combiner.StatsCombiner
    Combiner that keeps track of min, max, sum, and count
    ----------> set StatsCombiner parameter all, set to true to apply Combiner to every column, otherwise leave blank. if true, columns option will be ignored.: 
    ----------> set StatsCombiner parameter columns, <col fam>[:<col qual>]{,<col fam>[:<col qual>]} escape non aplhanum chars using %<hex>.: hstat
    ----------> set StatsCombiner parameter radix, radix/base of the numbers: 16
    username@instance runners> insert 123456 name first Joe
    username@instance runners> insert 123456 stat marathon 240
    username@instance runners> scan
    123456 name:first []    Joe
    123456 stat:marathon []    240,240,240,1
    username@instance runners> insert 123456 stat marathon 230
    username@instance runners> insert 123456 stat marathon 220
    username@instance runners> scan
    123456 name:first []    Joe
    123456 stat:marathon []    220,240,690,3
    username@instance runners> insert 123456 hstat virtualMarathon 6a
    username@instance runners> insert 123456 hstat virtualMarathon 6b
    username@instance runners> scan
    123456 hstat:virtualMarathon []    6a,6b,d5,2
    123456 name:first []    Joe
    123456 stat:marathon []    220,240,690,3

In this example a table is created and the example stats combiner is applied to
the column family stat and hstat.  The stats combiner computes min,max,sum, and
count.  It can be configured to use a different base or radix.  In the example
above the column family stat is configured for base 10 and the column family
hstat is configured for base 16.
