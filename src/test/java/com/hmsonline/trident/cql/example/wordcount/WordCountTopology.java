package com.hmsonline.trident.cql.example.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.hmsonline.trident.cql.CassandraCqlMapState;
import com.hmsonline.trident.cql.MapConfiguredCqlClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;

public class WordCountTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountTopology.class);

    @SuppressWarnings("unchecked")
    public static StormTopology buildWordCountTopology(LocalDRPC drpc) {
        LOG.info("Building topology.");
        TridentTopology topology = new TridentTopology();

        FixedBatchSpout spout1 = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        spout1.setCycle(true);

        // Quick note on an interesting error I encountered regarding using Count() as an aggregator:
        // Count utilizes an aggregator that updates values with a Long type. As you might notice, schema.cql
        // stores the count as an integer datatype within the wordcounttable. To remedy this issue, ensure
        // that your input fields datatypes are consistent with your persistent aggregation function. 
        // IntegerCount is exactly the same as storm.trident.operation.builtin.Count but instead has the
        // Integer as its field.
        TridentState wordCounts =
                topology.newStream("spout1", spout1)
                        .each(new Fields("sentence"), new Split(), new Fields("word"))
                        .groupBy(new Fields("word"))
                        .persistentAggregate(CassandraCqlMapState.nonTransactional(new WordCountMapper()),
                                new IntegerCount(), new Fields("count"))
                        .parallelismHint(6);

        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

        return topology.build();
    }

    @SuppressWarnings("unchecked")
    public static StormTopology buildWordCountAndSourceTopology(LocalDRPC drpc) {
        LOG.info("Building topology.");
        TridentTopology topology = new TridentTopology();

        String source1 = "spout1";
        String source2 = "spout2";
        FixedBatchSpout spout1 = new FixedBatchSpout(new Fields("sentence", "source"), 3,
                new Values("the cow jumped over the moon", source1),
                new Values("the man went to the store and bought some candy", source1),
                new Values("four score and seven years ago", source2),
                new Values("how many apples can you eat", source2));
        spout1.setCycle(true);

        TridentState wordCounts =
                topology.newStream("spout1", spout1)
                        .each(new Fields("sentence"), new Split(), new Fields("word"))
                        .groupBy(new Fields("word", "source"))
                        .persistentAggregate(CassandraCqlMapState.nonTransactional(new WordCountAndSourceMapper()),
                                new IntegerCount(), new Fields("count"))
                        .parallelismHint(6);

        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        final Config configuration = new Config();
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CQL_HOSTS, "localhost");
        final LocalCluster cluster = new LocalCluster();
        LocalDRPC client = new LocalDRPC();

        LOG.info("Submitting topology.");
        cluster.submitTopology("cqlexample", configuration, buildWordCountAndSourceTopology(client));
        LOG.info("Topology submitted.");
        Thread.sleep(100000);
        LOG.info("DRPC Query: Word Count [cat, dog, the, man]: {}", client.execute("words", "cat dog the man"));
        cluster.shutdown();
        client.shutdown();
    }
}
