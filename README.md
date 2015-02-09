Description / Rationale
===================

This is a new CassandraState implementation built on the CQL java driver.  For Cassandra, CQL has better support for lightweight transacations, batching, and collections.  
Also, CQL will likely get more attention than the legacy Thrift interface.  For these reasons, we decided to create a C* state implementation built on CQL.

Design
===================
An application/topology provides implementations of the mapper interfaces. 
For example, the [CqlRowMapper](https://github.com/hmsonline/storm-cassandra-cql/blob/master/src/main/java/com/hmsonline/trident/cql/mappers/CqlRowMapper.java) provides a bidirectional mapping from Keys and Values to statements that can be used to upsert and retrieve data.

Getting Started
===================
You can use the examples to get started.  For the example, you'll want to run a local cassandra instance with the example schema found in 
[schema.cql](https://github.com/hmsonline/storm-cassandra-cql/blob/master/src/test/resources/schema.cql).

You can do this using cqlsh:

```
cat storm-cassandra-cql/src/test/resources/create_keyspace.cql | cqlsh
cat storm-cassandra-cql/src/test/resources/schema.cql | cqlsh
```


## SimpleUpdateTopology

The SimpleUpdateTopology simply emits integers (0-99) and writes those to Cassandra with the current timestamp % 10.  The values are written to the table: mytable.

This is persistence from the topology:
```java
   inputStream.partitionPersist(new CassandraCqlStateFactory(ConsistencyLevel.ONE), new Fields("test"), new CassandraCqlStateUpdater(mapper));
```

During a partition persist, Storm repeatedly calls `updateState()` on the CassandraCqlStateUpdater to update a state object for the batch.  The updater uses the mapper to convert the tuple into a CQL statement, and caches the CQL statement in a CassandraCqlState object.  When the batch is complete, Storm calls commit on the state object, which then executes all of the CQL statements as a batch.

See: 
* [CassandraCqlStateUpdater.updateState](https://github.com/hmsonline/storm-cassandra-cql/blob/master/src/main/java/com/hmsonline/trident/cql/CassandraCqlStateUpdater.java#L37-L41)
* [CassandraCqlState.commit](https://github.com/hmsonline/storm-cassandra-cql/blob/master/src/main/java/com/hmsonline/trident/cql/CassandraCqlState.java#L39-L56)

The SimpleUpdateMapper looksl like this:

```java
public class SimpleUpdateMapper implements CqlRowMapper<Object, Object>, Serializable {
    public Statement map(TridentTuple tuple) {
        long t = System.currentTimeMillis() % 10;
        Update statement = update("mykeyspace", "mytable");
        statement.with(set("col1", tuple.getString(0))).where(eq("t", t));
        return statement;
    }
```

As you can see, it maps tuples to update statements.

When you run the `main()` method in the SimpleUpdateTopology, you should get results in mytable that look like this:
 
```
 t | col1
---+------
 5 |   97
 1 |   99
 8 |   99
 0 |   99
 2 |   99
 4 |   99
 7 |   99
 6 |   99
 9 |   99
 3 |   99
```


## WordCountTopology
The first example is a WordCount example.  This uses the CassandraCqlMapState, which writes/reads keys and values to/from Cassandra.

Have a look at the [WordCountTopology](https://github.com/hmsonline/storm-cassandra-cql/blob/master/src/test/java/com/hmsonline/trident/cql/example/wordcount/WordCountTopology.java).

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

Notice that an instance of WordCountMapper is passed into the CassandraCqlMapState.





