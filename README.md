Description / Rationale
===================

This is a new CassandraState implementation built on the CQL java driver.  For Cassandra, CQL has better support for lightweight transacations, batching, and collections.  Also, CQL will likely get more attention than the legacy Thrift interface.  For these reasons, we decided to create a C* state implementation built on CQL.

Storm-Cassandra-Cql provides three different state implementations:
* CassandraCqlState : Simply maps tuples to statements with batching capabilities.
* CassandraCqlMapState : Provides an IBackingMap implementation for use with keys/values and aggregations in Storm.
* CassandraCqlIncrementalState : Leverages conditional updates to perform dimensional aggregations on data incrementally. (each batch constitutes an increment)

Design
===================
An application/topology provides implementations of the mapper interfaces. 
For example, the [CqlRowMapper](https://github.com/hmsonline/storm-cassandra-cql/blob/master/src/main/java/com/hmsonline/trident/cql/mappers/CqlRowMapper.java) provides a bidirectional mapping from Keys and Values to statements that can be used to upsert and retrieve data.

The mappers are used to translate between Storm constructs and CQL constructs.  Storm uses the state factories to create state objects.  Updaters then use the mappers to update the state objects.  State is then committed on a per batch basis.

Getting Started
===================
You can use the examples to get started.  For the example, you'll want to run a local cassandra instance with the example schema found in 
[schema.cql](https://github.com/hmsonline/storm-cassandra-cql/blob/master/src/test/resources/schema.cql).

You can do this using cqlsh:

```
cat storm-cassandra-cql/src/test/resources/create_keyspace.cql | cqlsh
cat storm-cassandra-cql/src/test/resources/schema.cql | cqlsh
```


## SimpleUpdateTopology (CassandraCqlState)

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


## WordCountTopology (CassandraCqlMapState)
The WordCountTopology is slightly more complex in that it uses the CassandraCqlMapState.  The map state assumes you are reading/writing keys and values.  
The topology emits words from two different sources, and then totals the words by source and persists the count to Cassandra.

The CassandraCqlMapState object implements the IBackingMap interface in Storm.
See:
[Blog on the use of IBackingMap](https://svendvanderveken.wordpress.com/2013/07/30/scalable-real-time-state-update-with-storm/)

Have a look at the [WordCountTopology](https://github.com/hmsonline/storm-cassandra-cql/blob/master/src/test/java/com/hmsonline/trident/cql/example/wordcount/WordCountTopology.java).

It uses a FixedBatchSpout that emits sentences over and over again:

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

Instead of a partitionPersist like the other topology, this topology aggregates first, using the persistentAggregate method.  This performs an aggregation, storing the results in the CassandraCqlMapState, on which Storm eventually calls multiPut/multiGet to store/read values.

See:
[CassandraCqlMapState.multiPut/Get](https://github.com/hmsonline/storm-cassandra-cql/blob/master/src/main/java/com/hmsonline/trident/cql/CassandraCqlMapState.java#L122-L187)

In this case, the mapper maps keys and values to CQL statements:

```java
    @Override
    public Statement map(List<String> keys, Number value) {
        Insert statement = QueryBuilder.insertInto(KEYSPACE_NAME, TABLE_NAME);
        statement.value(WORD_KEY_NAME, keys.get(0));
        statement.value(SOURCE_KEY_NAME, keys.get(1));
        statement.value(VALUE_NAME, value);
        return statement;
    }

    @Override
    public Statement retrieve(List<String> keys) {
        // Retrieve all the columns associated with the keys
        Select statement = QueryBuilder.select().column(SOURCE_KEY_NAME)
                .column(WORD_KEY_NAME).column(VALUE_NAME)
                .from(KEYSPACE_NAME, TABLE_NAME);
        statement.where(QueryBuilder.eq(SOURCE_KEY_NAME, keys.get(0)));
        statement.where(QueryBuilder.eq(WORD_KEY_NAME, keys.get(1)));
        return statement;
    }
```

When you run this example, you will find the following counts in Cassandra:

```
cqlsh> select * from mykeyspace.wordcounttable;

 source | word   | count
--------+--------+-------
 spout2 |      a |    73
 spout2 |    ago |    74
 spout2 |    and |    74
 spout2 | apples |    69
...
 spout1 |   some |    74
 spout1 |  store |    74
 spout1 |    the |   296
 spout1 |     to |    74
 spout1 |   went |    74
```

## SalesTopology (CassandraCqlIncrementalState)
The SalesTopology demonstrates the use of Cassandra for incremental data aggregation.  The emitter/spout simulates sales of products across states.  It emits three fields: state, product and price.  The state object incorporates an aggregator, and aggregates values as tuples are processed.  It then flushes the aggregate values to Cassandra, by first reading the current value, incorporating the new value, and then writing the new value using a conditional update.


Here is the topolgy:

```java
   Stream inputStream = topology.newStream("sales", spout);
   SalesMapper mapper = new SalesMapper();
   inputStream.partitionPersist(
      new CassandraCqlIncrementalStateFactory<String, Number>(new Sum(), mapper),
      new Fields("price", "state", "product"),
      new CassandraCqlIncrementalStateUpdater<String, Number>());
```

Notice that the constructor for the state factory takes an aggregation, along with the mapper.  As the updater processes the tuples, the aggregator is used to maintain aggregate values based on the keys. The updater users a standard mapper.  For this example, the mapper is as follows:

```java
    @Override
    public Statement read(String key) {
        Select statement = select().column(VALUE_NAME).from(KEYSPACE_NAME, TABLE_NAME);
        statement.where(eq(KEY_NAME, key));
        return statement;
    }

    @Override
    public Statement update(String key, Number value, PersistedState<Number> state, long txid, int partition) {
        Update update = QueryBuilder.update(KEYSPACE_NAME, TABLE_NAME);
        update.with(set(VALUE_NAME, value)).where(eq(KEY_NAME, key));
        if (state.getValue() != null) {
            update.onlyIf(eq(VALUE_NAME, state.getValue()));
        }
        return update;
    }

    @Override
    public SalesState currentState(String key, List<Row> rows) {
        if (rows.size() == 0) {
            return new SalesState(null, null);
        } else {
            return new SalesState(rows.get(0).getInt(VALUE_NAME), "");
        }
    }

    @Override
    public String getKey(TridentTuple tuple) {
        String state = tuple.getString(1);
        return state;
    }

    @Override
    public Number getValue(TridentTuple tuple) {
        return tuple.getInteger(0);
    }
```

Notice the update statement includes a condition.  Also notice that the mapper provides a way to retrieve the relevant key and value from the tuple.  These methods are used by the updater to get the key and value to incorporate into the aggregation.


