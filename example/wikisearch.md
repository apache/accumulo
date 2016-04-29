---
title: The wikipedia search example explained, with performance numbers.
---

## Apache Accumulo Query Performance

## Sample Application

Starting with release 1.4, Accumulo includes an example web application that
provides a flexible, scalable search over the articles of Wikipedia, a widely
available medium-sized corpus.

The example uses an indexing technique helpful for doing multiple logical tests
against content. In this case, we can perform a word search on Wikipedia
articles. The sample application takes advantage of 3 unique capabilities of
Accumulo:

1. Extensible iterators that operate within the distributed tablet servers of
   the key-value store
1. Custom aggregators which can efficiently condense information during the
   various life-cycles of the log-structured merge tree 
1. Custom load balancing, which ensures that a table is evenly distributed on
   all tablet servers

In the example, Accumulo tracks the cardinality of all terms as elements are
ingested. If the cardinality is small enough, it will track the set of
documents by term directly. For example:

<style type="text/css">
table.wiki, table.wiki td, table.wiki th {
  padding-right: 5px;
  padding-left: 5px;
  border: 1px solid black;
  border-collapse: collapse;
}
table.wiki td {
  text-align: right;
}
</style>

{: .wiki }
| Row (word) | Value (count) | Value (document list)       |
|------------|--------------:|:----------------------------|
| Octopus    | 2             | [Document 57, Document 220] |
| Other      | 172,849       | []                          |
| Ostrich    | 1             | [Document 901]              |

Searches can be optimized to focus on low-cardinality terms. To create these
counts, the example installs "aggregators" which are used to combine inserted
values. The ingester just writes simple "(Octopus, 1, Document 57)" tuples.
The tablet servers then used the installed aggregators to merge the cells as
the data is re-written, or queried. This reduces the in-memory locking
required to update high-cardinality terms, and defers aggregation to a later
time, where it can be done more efficiently.

The example also creates a reverse word index to map each word to the document
in which it appears. But it does this by choosing an arbitrary partition for
the document. The article, and the word index for the article are grouped
together into the same partition. For example:

{: .wiki }
| Row (partition) | Column Family | Column Qualifier | Value           |
|-----------------|---------------|------------------|-----------------|
| 1               | D             | Document 57      | "smart Octopus" |
| 1               | Word, Octopus | Document 57      |                 |
| 1               | Word, smart   | Document 57      |                 |
| ...             |               |                  |                 |
| 2               | D             | Document 220     | "big Octopus"   |
| 2               | Word, big     | Document 220     |                 |
| 2               | Word, Octopus | Document 220     |                 |

Of course, there would be large numbers of documents in each partition, and the
elements of those documents would be interlaced according to their sort order.

By dividing the index space into partitions, the multi-word searches can be
performed in parallel across all the nodes. Also, by grouping the document
together with its index, a document can be retrieved without a second request
from the client. The query "octopus" and "big" will be performed on all the
servers, but only those partitions for which the low-cardinality term "octopus"
can be found by using the aggregated reverse index information. The query for a
document is performed by extensions provided in the example. These extensions
become part of the tablet server's iterator stack. By cloning the underlying
iterators, the query extensions can seek to specific words within the index,
and when it finds a matching document, it can then seek to the document
location and retrieve the contents.

We loaded the example on a cluster of 10 servers, each with 12 cores, and 32G
RAM, 6 500G drives. Accumulo tablet servers were allowed a maximum of 3G of
working memory, of which 2G was dedicated to caching file data.

Following the instructions in the example, the Wikipedia XML data for articles
was loaded for English, Spanish and German languages into 10 partitions. The
data is not partitioned by language: multiple languages were used to get a
larger set of test data. The data load took around 8 hours, and has not been
optimized for scale. Once the data was loaded, the content was compacted which
took about 35 minutes.

The example uses the language-specific tokenizers available from the Apache
Lucene project for Wikipedia data.

Original files:

{: .wiki }
| Articles | Compressed size | Filename                               |
|----------|-----------------|----------------------------------------|
| 1.3M     | 2.5G            | dewiki-20111120-pages-articles.xml.bz2 |
| 3.8M     | 7.9G            | enwiki-20111115-pages-articles.xml.bz2 |
| 0.8M     | 1.4G            | eswiki-20111112-pages-articles.xml.bz2 |

The resulting tables:

    > du -p wiki.*
          47,325,680,634 [wiki]
           5,125,169,305 [wikiIndex]
                     413 [wikiMetadata]
           5,521,690,682 [wikiReverseIndex]

Roughly a 6:1 increase in size.

We performed the following queries, and repeated the set 5 times. The query
language is much more expressive than what is shown below. The actual query
specified that these words were to be found in the body of the article. Regular
expressions, searches within titles, negative tests, etc are available.

{: .wiki }
| Query                                   | Sample 1 (seconds) | Sample 2 (seconds) | Sample 3 (seconds) | Sample 4 (seconds) | Sample 5 (seconds) | Matches | Result Size |
|-----------------------------------------|------|------|------|------|------|--------|-----------|
| "old" and "man" and "sea"               | 4.07 | 3.79 | 3.65 | 3.85 | 3.67 | 22,956 | 3,830,102 |
| "paris" and "in" and "the" and "spring" | 3.06 | 3.06 | 2.78 | 3.02 | 2.92 | 10,755 | 1,757,293 |
| "rubber" and "ducky" and "ernie"        | 0.08 | 0.08 | 0.1  | 0.11 | 0.1  | 6      | 808       |
| "fast" and ( "furious" or "furriest")   | 1.34 | 1.33 | 1.3  | 1.31 | 1.31 | 2,973  | 493,800   |
| "slashdot" and "grok"                   | 0.06 | 0.06 | 0.06 | 0.06 | 0.06 | 14     | 2,371     |
| "three" and "little" and "pigs"         | 0.92 | 0.91 | 0.9  | 1.08 | 0.88 | 2,742  | 481,531   |

Because the terms are tested together within the tablet server, even fairly
high-cardinality terms such as "old," "man," and "sea" can be tested
efficiently, without needing to return to the client, or make distributed calls
between servers to perform the intersection between terms.

For reference, here are the cardinalities for all the terms in the query
(remember, this is across all languages loaded):

{: .wiki }
| Term     | Cardinality |
|----------|-------------|
| ducky    | 795         |
| ernie    | 13,433      |
| fast     | 166,813     |
| furious  | 10,535      |
| furriest | 45          |
| grok     | 1,168       |
| in       | 1,884,638   |
| little   | 320,748     |
| man      | 548,238     |
| old      | 720,795     |
| paris    | 232,464     |
| pigs     | 8,356       |
| rubber   | 17,235      |
| sea      | 247,231     |
| slashdot | 2,343       |
| spring   | 125,605     |
| the      | 3,509,498   |
| three    | 718,810     |

Accumulo supports caching index information, which is turned on by default, and
for the non-index blocks of a file, which is not. After turning on data block
  caching for the wiki table:

{: .wiki }
| Query                                   | Sample 1 (seconds) | Sample 2 (seconds) | Sample 3 (seconds) | Sample 4 (seconds) | Sample 5 (seconds) |
|-----------------------------------------|------|------|------|------|------|
| "old" and "man" and "sea"               | 2.47 | 2.48 | 2.51 | 2.48 | 2.49 |
| "paris" and "in" and "the" and "spring" | 1.33 | 1.42 | 1.6  | 1.61 | 1.47 |
| "rubber" and "ducky" and "ernie"        | 0.07 | 0.08 | 0.07 | 0.07 | 0.07 |
| "fast" and ( "furious" or "furriest")   | 1.28 | 0.78 | 0.77 | 0.79 | 0.78 |
| "slashdot" and "grok"                   | 0.04 | 0.04 | 0.04 | 0.04 | 0.04 |
| "three" and "little" and "pigs"         | 0.55 | 0.32 | 0.32 | 0.31 | 0.27 |

For comparison, these are the cold start lookup times (restart Accumulo, and
drop the operating system disk cache):

{: .wiki }
| Query                                   | Sample |
|-----------------------------------------|--------|
| "old" and "man" and "sea"               | 13.92  |
| "paris" and "in" and "the" and "spring" | 8.46   |
| "rubber" and "ducky" and "ernie"        | 2.96   |
| "fast" and ( "furious" or "furriest")   | 6.77   |
| "slashdot" and "grok"                   | 4.06   |
| "three" and "little" and "pigs"         | 8.13   |

### Random Query Load

Random queries were generated using common english words. A uniform random
sample of 3 to 5 words taken from the 10000 most common words in the Project
Gutenberg's online text collection were joined with "and". Words containing
anything other than letters (such as contractions) were not used. A client was
started simultaneously on each of the 10 servers and each ran 100 random
queries (1000 queries total).

{: .wiki }
| Time  | Count   |
|-------|---------|
| 41.97 | 440,743 |
| 41.61 | 320,522 |
| 42.11 | 347,969 |
| 38.32 | 275,655 |

### Query Load During Ingest

The English wikipedia data was re-ingested on top of the existing, compacted
data. The following query samples were taken in 5 minute intervals while
ingesting 132 articles/second:

{: .wiki }
| Query                                   | Sample 1 (seconds)  | Sample 2 (seconds) | Sample 3 (seconds) | Sample 4 (seconds) | Sample 5 (seconds) |
|-----------------------------------------|------|------|-------|------|-------|
| "old" and "man" and "sea"               | 4.91 | 3.92 | 11.58 | 9.86 | 10.21 |
| "paris" and "in" and "the" and "spring" | 5.03 | 3.37 | 12.22 | 3.29 | 9.46  |
| "rubber" and "ducky" and "ernie"        | 4.21 | 2.04 | 8.57  | 1.54 | 1.68  |
| "fast" and ( "furious" or "furriest")   | 5.84 | 2.83 | 2.56  | 3.12 | 3.09  |
| "slashdot" and "grok"                   | 5.68 | 2.62 | 2.2   | 2.78 | 2.8   |
| "three" and "little" and "pigs"         | 7.82 | 3.42 | 2.79  | 3.29 | 3.3   |
