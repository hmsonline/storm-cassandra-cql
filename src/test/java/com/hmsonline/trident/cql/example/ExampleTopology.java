package com.hmsonline.trident.cql.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.hmsonline.trident.cql.CassandraCqlStateFactory;
import com.hmsonline.trident.cql.CassandraCqlStateUpdater;

public class ExampleTopology {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleTopology.class);

    public static StormTopology buildTopology() {
        LOG.info("Building topology.");
        TridentTopology topology = new TridentTopology();
        ExampleSpout spout = new ExampleSpout();
        Stream inputStream = topology.newStream("test", spout);
        ExampleMapper mapper = new ExampleMapper();
        inputStream.partitionPersist(new CassandraCqlStateFactory(), new Fields("test"), new CassandraCqlStateUpdater(mapper));
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        final Config configuration = new Config();
        configuration.put(CassandraCqlStateFactory.TRIDENT_CASSANDRA_CQL_HOSTS, "localhost");
        final LocalCluster cluster = new LocalCluster();
        LOG.info("Submitting topology.");
        cluster.submitTopology("cqlexample", configuration, buildTopology());
        LOG.info("Topology submitted.");
        Thread.sleep(600000);
    }
}
