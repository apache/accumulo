---
title: The wikipedia search example explained, with performance numbers.
---

Apache Accumulo Query Performance
--------------------------

Sample Application
------------------

Starting with release 1.4, Accumulo includes an example web application that provides a flexible,  scalable search over the articles of Wikipedia, a widely available medium-sized corpus.

The example uses an indexing technique helpful for doing multiple logical tests against content.  In this case, we can perform a word search on Wikipedia articles.   The sample application takes advantage of 3 unique capabilities of Accumulo:

 1. Extensible iterators that operate within the distributed tablet servers of the key-value store
 2. Custom aggregators which can efficiently condense information during the various life-cycles of the log-structured merge tree 
 3. Custom load balancing, which ensures that a table is evenly distributed on all tablet servers

In the example, Accumulo tracks the cardinality of all terms as elements are ingested.  If the cardinality is small enough, it will track the set of documents by term directly.  For example:

<style type="text/css">
table, td, th {
  padding-right: 5px;
  padding-left: 5px;
  border: 1px solid black;
  border-collapse: collapse;
}
td {
  text-align: right;
}
.lt {
  text-align: left;
}
</style>

<table>
<tr>
<th>Row (word)</th>
<th colspan="2">Value (count, document list)</th>
</tr><tr>
<td>Octopus
<td>2
<td class='lt'>[Document 57, Document 220]
</tr><tr>
<td>Other
<td>172,849
<td class='lt'>[]
</tr><tr>
<td>Ostrich
<td>1
<td class='lt'>[Document 901]
</tr>
</table>

Searches can be optimized to focus on low-cardinality terms.  To create these counts, the example installs “aggregators” which are used to combine inserted values.  The ingester just writes simple  “(Octopus, 1, Document 57)” tuples.  The tablet servers then used the installed aggregators to merge the cells as the data is re-written, or queried.  This reduces the in-memory locking required to update high-cardinality terms, and defers aggregation to a later time, where it can be done more efficiently.

The example also creates a reverse word index to map each word to the document in which it appears. But it does this by choosing an arbitrary partition for the document.  The article, and the word index for the article are grouped together into the same partition.  For example:

<table>
<tr>
<th>Row (partition)
<th>Column Family
<th>Column Qualifier
<th>Value
<tr>
<td>1
<td>D
<td>Document 57
<td>“smart Octopus”
<tr>
<td>1
<td>Word, Octopus
<td>Document 57
<td>
<tr>
<td>1
<td>Word, smart
<td>Document 57
<td>
<tr>
<td>...
<td>
<td>
<td>
<tr>
<td>2
<td>D
<td>Document 220
<td>“big Octopus”
<tr>
<td>2
<td>Word, big
<td>Document 220
<td>
<tr>
<td>2
<td>Word, Octopus
<td>Document 220
<td>
</table>

Of course, there would be large numbers of documents in each partition, and the elements of those documents would be interlaced according to their sort order.

By dividing the index space into partitions, the multi-word searches can be performed in parallel across all the nodes.  Also, by grouping the document together with its index, a document can be retrieved without a second request from the client.  The query “octopus” and “big” will be performed on all the servers, but only those partitions for which the low-cardinality term “octopus” can be found by using the aggregated reverse index information.  The query for a document is performed by extensions provided in the example.  These extensions become part of the tablet server's iterator stack.  By cloning the underlying iterators, the query extensions can seek to specific words within the index, and when it finds a matching document, it can then seek to the document location and retrieve the contents.

We loaded the example on a  cluster of 10 servers, each with 12 cores, and 32G RAM, 6 500G drives.  Accumulo tablet servers were allowed a maximum of 3G of working memory, of which 2G was dedicated to caching file data.

Following the instructions in the example, the Wikipedia XML data for articles was loaded for English, Spanish and German languages into 10 partitions.  The data is not partitioned by language: multiple languages were used to get a larger set of test data.  The data load took around 8 hours, and has not been optimized for scale.  Once the data was loaded, the content was compacted which took about 35 minutes.

The example uses the language-specific tokenizers available from the Apache Lucene project for Wikipedia data.

Original files:

<table>
<tr>
<th>Articles
<th>Compressed size
<th>Filename
<tr>
<td>1.3M
<td>2.5G
<td>dewiki-20111120-pages-articles.xml.bz2
<tr>
<td>3.8M
<td>7.9G
<td>enwiki-20111115-pages-articles.xml.bz2
<tr>
<td>0.8M
<td>1.4G
<td>eswiki-20111112-pages-articles.xml.bz2
</table>

The resulting tables:

    > du -p wiki.*
          47,325,680,634 [wiki]
           5,125,169,305 [wikiIndex]
                     413 [wikiMetadata]
           5,521,690,682 [wikiReverseIndex]

Roughly a 6:1 increase in size.

We performed the following queries, and repeated the set 5 times.  The query language is much more expressive than what is shown below.  The actual query specified that these words were to be found in the body of the article.  Regular expressions, searches within titles, negative tests, etc are available.

<table>
<tr>
<th>Query
<th colspan="5">Samples (seconds)
<th>Matches
<th>Result Size
<tr>
<td>“old” and “man” and “sea”
<td>4.07
<td>3.79
<td>3.65
<td>3.85
<td>3.67
<td>22,956
<td>3,830,102
<tr>
<td>“paris” and “in” and “the” and “spring”
<td>3.06
<td>3.06
<td>2.78
<td>3.02
<td>2.92
<td>10,755
<td>1,757,293
<tr>
<td>“rubber” and “ducky” and “ernie”
<td>0.08
<td>0.08
<td>0.1
<td>0.11
<td>0.1
<td>6
<td>808
<tr>
<td>“fast”  and ( “furious” or “furriest”) 
<td>1.34
<td>1.33
<td>1.3
<td>1.31
<td>1.31
<td>2,973
<td>493,800
<tr>
<td>“slashdot” and “grok”
<td>0.06
<td>0.06
<td>0.06
<td>0.06
<td>0.06
<td>14
<td>2,371
<tr>
<td>“three” and “little” and “pigs”
<td>0.92
<td>0.91
<td>0.9
<td>1.08
<td>0.88
<td>2,742
<td>481,531
</table>

Because the terms are tested together within the tablet server, even fairly high-cardinality terms such as “old,” “man,” and “sea” can be tested efficiently, without needing to return to the client, or make distributed calls between servers to perform the intersection between terms.

For reference, here are the cardinalities for all the terms in the query (remember, this is across all languages loaded):

<table>
<tr> <th>Term <th> Cardinality
<tr> <td> ducky <td> 795
<tr> <td> ernie <td> 13,433
<tr> <td> fast <td> 166,813
<tr> <td> furious <td> 10,535
<tr> <td> furriest <td> 45
<tr> <td> grok <td> 1,168
<tr> <td> in <td> 1,884,638
<tr> <td> little <td> 320,748
<tr> <td> man <td> 548,238
<tr> <td> old <td> 720,795
<tr> <td> paris <td> 232,464
<tr> <td> pigs <td> 8,356
<tr> <td> rubber <td> 17,235
<tr> <td> sea <td> 247,231
<tr> <td> slashdot <td> 2,343
<tr> <td> spring <td> 125,605
<tr> <td> the <td> 3,509,498
<tr> <td> three <td> 718,810
</table>


Accumulo supports caching index information, which is turned on by default, and for the non-index blocks of a file, which is not. After turning on data block caching for the wiki table:

<table>
<tr>
<th>Query
<th colspan="5">Samples (seconds)
<tr>
<td>“old” and “man” and “sea”
<td>2.47
<td>2.48
<td>2.51
<td>2.48
<td>2.49
</tr><tr>
<td>“paris” and “in” and “the” and “spring”
<td>1.33
<td>1.42
<td>1.6
<td>1.61
<td>1.47
</tr><tr>
<td>“rubber” and “ducky” and “ernie”
<td>0.07
<td>0.08
<td>0.07
<td>0.07
<td>0.07
</tr><tr>
<td>“fast” and ( “furious” or “furriest”) 
<td>1.28
<td>0.78
<td>0.77
<td>0.79
<td>0.78
</tr><tr>
<td>“slashdot” and “grok”
<td>0.04
<td>0.04
<td>0.04
<td>0.04
<td>0.04
</tr><tr>
<td>“three” and “little” and “pigs”
<td>0.55
<td>0.32
<td>0.32
<td>0.31
<td>0.27
</tr>
<table>
<p>
For comparison, these are the cold start lookup times (restart Accumulo, and drop the operating system disk cache):

<table>
<tr>
<th>Query
<th>Sample
<tr>
<td>“old” and “man” and “sea”
<td>13.92
<tr>
<td>“paris” and “in” and “the” and “spring”
<td>8.46
<tr>
<td>“rubber” and “ducky” and “ernie”
<td>2.96
<tr>
<td>“fast” and ( “furious” or “furriest”) 
<td>6.77
<tr>
<td>“slashdot” and “grok”
<td>4.06
<tr>
<td>“three” and “little” and “pigs”
<td>8.13
</table>

### Random Query Load

Random queries were generated using common english words.  A uniform random sample of 3 to 5 words taken from the 10000 most common words in the Project Gutenberg's online text collection were joined with “and”.  Words containing anything other than letters (such as contractions) were not used.  A client was started simultaneously on each of the 10 servers and each ran 100 random queries (1000 queries total).


<table>
<tr>
<th>Time
<th>Count
<tr>
<td>41.97
<td>440,743
<tr>
<td>41.61
<td>320,522
<tr>
<td>42.11
<td>347,969
<tr>
<td>38.32
<td>275,655
</table>

### Query Load During Ingest

The English wikipedia data was re-ingested on top of the existing, compacted data. The following  query samples were taken in 5 minute intervals while ingesting 132 articles/second:


<table>
<tr>
<th>Query
<th colspan="5">Samples (seconds)
<tr>
<td>“old” and “man” and “sea”
<td>4.91
<td>3.92
<td>11.58
<td>9.86
<td>10.21
<tr>
<td>“paris” and “in” and “the” and “spring”
<td>5.03
<td>3.37
<td>12.22
<td>3.29
<td>9.46
<tr>
<td>“rubber” and “ducky” and “ernie”
<td>4.21
<td>2.04
<td>8.57
<td>1.54
<td>1.68
<tr>
<td>“fast”  and ( “furious” or “furriest”) 
<td>5.84
<td>2.83
<td>2.56
<td>3.12
<td>3.09
<tr>
<td>“slashdot” and “grok”
<td>5.68
<td>2.62
<td>2.2
<td>2.78
<td>2.8
<tr>
<td>“three” and “little” and “pigs”
<td>7.82
<td>3.42
<td>2.79
<td>3.29
<td>3.3
</table>
