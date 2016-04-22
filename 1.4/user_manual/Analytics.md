---
title: "User Manual: Analytics"
---

** Next:** [Security][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [High-Speed Ingest][6]   ** [Contents][8]**   
  
<a id="CHILD_LINKS"></a>**Subsections**

* [MapReduce][9]
* [Combiners][10]
* [Statistical Modeling][11]

* * *

## <a id="Analytics"></a> Analytics

Accumulo supports more advanced data processing than simply keeping keys sorted and performing efficient lookups. Analytics can be developed by using MapReduce and Iterators in conjunction with Accumulo tables. 

## <a id="MapReduce"></a> MapReduce

Accumulo tables can be used as the source and destination of MapReduce jobs. To use an Accumulo table with a MapReduce job (specifically with the new Hadoop API as of version 0.20), configure the job parameters to use the AccumuloInputFormat and AccumuloOutputFormat. Accumulo specific parameters can be set via these two format classes to do the following: 

* Authenticate and provide user credentials for the input 
* Restrict the scan to a range of rows 
* Restrict the input to a subset of available columns 

### <a id="Mapper_and_Reducer_classes"></a> Mapper and Reducer classes

To read from an Accumulo table create a Mapper with the following class parameterization and be sure to configure the AccumuloInputFormat. 
    
    
    class MyMapper extends Mapper<Key,Value,WritableComparable,Writable> {
        public void map(Key k, Value v, Context c) {
            // transform key and value data here
        }
    }
    

To write to an Accumulo table, create a Reducer with the following class parameterization and be sure to configure the AccumuloOutputFormat. The key emitted from the Reducer identifies the table to which the mutation is sent. This allows a single Reducer to write to more than one table if desired. A default table can be configured using the AccumuloOutputFormat, in which case the output table name does not have to be passed to the Context object within the Reducer. 
    
    
    class MyReducer extends Reducer<WritableComparable, Writable, Text, Mutation> {
    
        public void reduce(WritableComparable key, Iterable<Text> values, Context c) {
            
            Mutation m;
            
            // create the mutation based on input key and value
            
            c.write(new Text("output-table"), m);
        }
    }
    

The Text object passed as the output should contain the name of the table to which this mutation should be applied. The Text can be null in which case the mutation will be applied to the default table name specified in the AccumuloOutputFormat options. 

### <a id="AccumuloInputFormat_options"></a> AccumuloInputFormat options
    
    
    Job job = new Job(getConf());
    AccumuloInputFormat.setInputInfo(job,
            "user",
            "passwd".getBytes(),
            "table",
            new Authorizations());
    
    AccumuloInputFormat.setZooKeeperInstance(job, "myinstance",
            "zooserver-one,zooserver-two");
    

**Optional settings:**

To restrict Accumulo to a set of row ranges: 
    
    
    ArrayList<Range> ranges = new ArrayList<Range>();
    // populate array list of row ranges ...
    AccumuloInputFormat.setRanges(job, ranges);
    

To restrict accumulo to a list of columns: 
    
    
    ArrayList<Pair<Text,Text>> columns = new ArrayList<Pair<Text,Text>>();
    // populate list of columns
    AccumuloInputFormat.fetchColumns(job, columns);
    

To use a regular expression to match row IDs: 
    
    
    AccumuloInputFormat.setRegex(job, RegexType.ROW, "^.*");
    

### <a id="AccumuloOutputFormat_options"></a> AccumuloOutputFormat options
    
    
    boolean createTables = true;
    String defaultTable = "mytable";
    
    AccumuloOutputFormat.setOutputInfo(job,
            "user",
            "passwd".getBytes(),
            createTables,
            defaultTable);
    
    AccumuloOutputFormat.setZooKeeperInstance(job, "myinstance",
            "zooserver-one,zooserver-two");
    

**Optional Settings:**
    
    
    AccumuloOutputFormat.setMaxLatency(job, 300); // milliseconds
    AccumuloOutputFormat.setMaxMutationBufferSize(job, 5000000); // bytes
    

An example of using MapReduce with Accumulo can be found at   
accumulo/docs/examples/README.mapred 

## <a id="Combiners"></a> Combiners

Many applications can benefit from the ability to aggregate values across common keys. This can be done via Combiner iterators and is similar to the Reduce step in MapReduce. This provides the ability to define online, incrementally updated analytics without the overhead or latency associated with batch-oriented MapReduce jobs. 

All that is needed to aggregate values of a table is to identify the fields over which values will be grouped, insert mutations with those fields as the key, and configure the table with a combining iterator that supports the summarizing operation desired. 

The only restriction on an combining iterator is that the combiner developer should not assume that all values for a given key have been seen, since new mutations can be inserted at anytime. This precludes using the total number of values in the aggregation such as when calculating an average, for example. 

### <a id="Feature_Vectors"></a> Feature Vectors

An interesting use of combining iterators within an Accumulo table is to store feature vectors for use in machine learning algorithms. For example, many algorithms such as k-means clustering, support vector machines, anomaly detection, etc. use the concept of a feature vector and the calculation of distance metrics to learn a particular model. The columns in an Accumulo table can be used to efficiently store sparse features and their weights to be incrementally updated via the use of an combining iterator. 

## <a id="Statistical_Modeling"></a> Statistical Modeling

Statistical models that need to be updated by many machines in parallel could be similarly stored within an Accumulo table. For example, a MapReduce job that is iteratively updating a global statistical model could have each map or reduce worker reference the parts of the model to be read and updated through an embedded Accumulo client. 

Using Accumulo this way enables efficient and fast lookups and updates of small pieces of information in a random access pattern, which is complementary to MapReduce's sequential access model. 

* * *

** Next:** [Security][2] ** Up:** [Apache Accumulo User Manual Version 1.4][4] ** Previous:** [High-Speed Ingest][6]   ** [Contents][8]**

[2]: Security.html
[4]: accumulo_user_manual.html
[6]: High_Speed_Ingest.html
[8]: Contents.html
[9]: Analytics.html#MapReduce
[10]: Analytics.html#Combiners
[11]: Analytics.html#Statistical_Modeling
