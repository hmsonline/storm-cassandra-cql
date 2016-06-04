package com.hmsonline.trident.cql.example.simpleupdate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;

import com.datastax.driver.core.ConsistencyLevel;
import com.hmsonline.trident.cql.CassandraCqlStateFactory;
import com.hmsonline.trident.cql.CassandraCqlStateUpdater;
import com.hmsonline.trident.cql.MapConfiguredCqlClientFactory;

public class SimpleUpdateTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleUpdateTopology.class);

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static StormTopology buildTopology() {
        LOG.info("Building topology.");
        TridentTopology topology = new TridentTopology();
        SimpleUpdateSpout spout = new SimpleUpdateSpout();
        Stream inputStream = topology.newStream("test", spout);
        SimpleUpdateMapper mapper = new SimpleUpdateMapper();
        inputStream.partitionPersist(new CassandraCqlStateFactory(ConsistencyLevel.ONE), new Fields("test"), new CassandraCqlStateUpdater(mapper));
        // inputStream.each(new Fields("test"), new Debug());
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        final Config configuration = new Config();
        configuration.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CQL_HOSTS, "localhost");
        final LocalCluster cluster = new LocalCluster();
        LOG.info("Submitting topology.");
        cluster.submitTopology("cqlexample", configuration, buildTopology());
        LOG.info("Topology submitted.");
        Thread.sleep(600000);
    }
}
