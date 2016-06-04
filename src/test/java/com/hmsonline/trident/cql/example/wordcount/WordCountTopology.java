package com.hmsonline.trident.cql.example.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import com.hmsonline.trident.cql.CassandraCqlMapState;
import com.hmsonline.trident.cql.MapConfiguredCqlClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;

public class WordCountTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountTopology.class);

    @SuppressWarnings("unchecked")
    public static StormTopology buildWordCountAndSourceTopology(LocalDRPC drpc) {
        LOG.info("Building topology.");
        TridentTopology topology = new TridentTopology();

        String source1 = "spout1";
        String source2 = "spout2";
        FixedBatchSpout spout1 = new FixedBatchSpout(new Fields("sentence", "source"), 3,
                new Values("the cow jumped over the moon", source1),
                new Values("the man went to the store and bought some candy", source1),
                new Values("four score and four years ago", source2),
                new Values("how much wood can a wood chuck chuck", source2));
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
        Thread.sleep(10000);
        LOG.info("DRPC Query: Word Count [cat, dog, the, man]: {}", client.execute("words", "cat dog the man"));
        cluster.shutdown();
        client.shutdown();
    }
}
