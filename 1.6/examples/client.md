---
title: Client Examples
---

This documents how you run the simplest java examples.

This tutorial uses the following Java classes, which can be found in org.apache.accumulo.examples.simple.client in the examples-simple module:

 * Flush.java - flushes a table
 * RowOperations.java - reads and writes rows
 * ReadWriteExample.java - creates a table, writes to it, and reads from it

Using the accumulo command, you can run the simple client examples by providing their
class name, and enough arguments to find your accumulo instance. For example,
the Flush class will flush a table:

    $ PACKAGE=org.apache.accumulo.examples.simple.client
    $ bin/accumulo $PACKAGE.Flush -u root -p mypassword -i instance -z zookeeper -t trace

The very simple RowOperations class demonstrates how to read and write rows using the BatchWriter
and Scanner:

    $ bin/accumulo $PACKAGE.RowOperations -u root -p mypassword -i instance -z zookeeper
    2013-01-14 14:45:24,738 [client.RowOperations] INFO : This is everything
    2013-01-14 14:45:24,744 [client.RowOperations] INFO : Key: row1 column:1 [] 1358192724640 false Value: This is the value for this key
    2013-01-14 14:45:24,744 [client.RowOperations] INFO : Key: row1 column:2 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,744 [client.RowOperations] INFO : Key: row1 column:3 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,744 [client.RowOperations] INFO : Key: row1 column:4 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,746 [client.RowOperations] INFO : Key: row2 column:1 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,746 [client.RowOperations] INFO : Key: row2 column:2 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,746 [client.RowOperations] INFO : Key: row2 column:3 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,746 [client.RowOperations] INFO : Key: row2 column:4 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,747 [client.RowOperations] INFO : Key: row3 column:1 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,747 [client.RowOperations] INFO : Key: row3 column:2 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,747 [client.RowOperations] INFO : Key: row3 column:3 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,747 [client.RowOperations] INFO : Key: row3 column:4 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,756 [client.RowOperations] INFO : This is row1 and row3
    2013-01-14 14:45:24,757 [client.RowOperations] INFO : Key: row1 column:1 [] 1358192724640 false Value: This is the value for this key
    2013-01-14 14:45:24,757 [client.RowOperations] INFO : Key: row1 column:2 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,757 [client.RowOperations] INFO : Key: row1 column:3 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,757 [client.RowOperations] INFO : Key: row1 column:4 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,761 [client.RowOperations] INFO : Key: row3 column:1 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,761 [client.RowOperations] INFO : Key: row3 column:2 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,761 [client.RowOperations] INFO : Key: row3 column:3 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,761 [client.RowOperations] INFO : Key: row3 column:4 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,765 [client.RowOperations] INFO : This is just row3
    2013-01-14 14:45:24,769 [client.RowOperations] INFO : Key: row3 column:1 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,770 [client.RowOperations] INFO : Key: row3 column:2 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,770 [client.RowOperations] INFO : Key: row3 column:3 [] 1358192724642 false Value: This is the value for this key
    2013-01-14 14:45:24,770 [client.RowOperations] INFO : Key: row3 column:4 [] 1358192724642 false Value: This is the value for this key

To create a table, write to it and read from it:

    $ bin/accumulo $PACKAGE.ReadWriteExample -u root -p mypassword -i instance -z zookeeper --createtable --create --read
    hello%00; datatypes:xml [LEVEL1|GROUP1] 1358192329450 false -> world
    hello%01; datatypes:xml [LEVEL1|GROUP1] 1358192329450 false -> world
    hello%02; datatypes:xml [LEVEL1|GROUP1] 1358192329450 false -> world
    hello%03; datatypes:xml [LEVEL1|GROUP1] 1358192329450 false -> world
    hello%04; datatypes:xml [LEVEL1|GROUP1] 1358192329450 false -> world
    hello%05; datatypes:xml [LEVEL1|GROUP1] 1358192329450 false -> world
    hello%06; datatypes:xml [LEVEL1|GROUP1] 1358192329450 false -> world
    hello%07; datatypes:xml [LEVEL1|GROUP1] 1358192329450 false -> world
    hello%08; datatypes:xml [LEVEL1|GROUP1] 1358192329450 false -> world
    hello%09; datatypes:xml [LEVEL1|GROUP1] 1358192329450 false -> world

