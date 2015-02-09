Description
===================

This is a new CassandraState implementation built on the CQL java driver.  

Rationale
===================

For Cassandra, CQL has better support for lightweight transacations, batching, and collections.  
Also, CQL will likely get more attention than the legacy Thrift interface.  For these reasons, we decided to create
a C* state implementation built on CQL.


Design
===================
An application/topology provides implementations of the mapper interfaces.  There are two types of Mappers, one that maps from Tuples to CQL, and the other maps from CQL to Tuples. 
The [CqlRowMapper](https://github.com/hmsonline/storm-cassandra-cql/blob/master/src/main/java/com/hmsonline/trident/cql/mappers/CqlRowMapper.java) maps from rows to values (for tuples).
The [CqlTupleMapper](https://github.com/hmsonline/storm-cassandra-cql/blob/master/src/main/java/com/hmsonline/trident/cql/mappers/CqlTupleMapper.java) maps tuples to CQL statements.

Getting Started
===================
You can use the examples to get started.  The first example is a WordCount example. For the example, you'll want to run a local cassandra instance.
Next, load the example schema found in [schema.cql](https://github.com/hmsonline/storm-cassandra-cql/blob/master/src/test/resources/schema.cql).
You can do this using cqlsh:

```
cat storm-cassandra-cql/src/test/resources/create_keyspace.cql | cqlsh
cat storm-cassandra-cql/src/test/resources/schema.cql | cqlsh
```

Then, have a look at the [WordCountTopology](https://github.com/hmsonline/storm-cassandra-cql/blob/master/src/test/java/com/hmsonline/trident/cql/example/wordcount/WordCountTopology.java).

It uses a FixedBatchSpout to emit sentences over and over again:

```java
   FixedBatchSpout spout1 = new FixedBatchSpout(new Fields("sentence"), 3,
      new Values("the cow jumped over the moon"),
      new Values("the man went to the store and bought some candy"),
      new Values("four score and seven years ago"),
      new Values("how many apples can you eat"));
   spout1.setCycle(true);
```

Then, it splits and groups those words:

```java
TridentState wordCounts =
                topology.newStream("spout1", spout1)
                        .each(new Fields("sentence"), new Split(), new Fields("word"))
                        .groupBy(new Fields("word"))
                        .persistentAggregate(CassandraCqlMapState.nonTransactional(new WordCountMapper()),
                                new IntegerCount(), new Fields("count"))
                        .parallelismHint(6);
```





