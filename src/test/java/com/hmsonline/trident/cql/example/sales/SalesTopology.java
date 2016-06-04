package com.hmsonline.trident.cql.example.sales;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;

import com.hmsonline.trident.cql.MapConfiguredCqlClientFactory;
import com.hmsonline.trident.cql.incremental.CassandraCqlIncrementalStateFactory;
import com.hmsonline.trident.cql.incremental.CassandraCqlIncrementalStateUpdater;

public class SalesTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SalesTopology.class);
    @Rule
    public static CassandraCQLUnit cqlUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("schema.cql","mykeyspace"));

    public static StormTopology buildTopology() {
        LOG.info("Building topology.");
        TridentTopology topology = new TridentTopology();
        SalesSpout spout = new SalesSpout();
        Stream inputStream = topology.newStream("sales", spout);
        SalesMapper mapper = new SalesMapper();
        inputStream.partitionPersist(
                new CassandraCqlIncrementalStateFactory<String, Number>(new Sum(), mapper),
                new Fields("price", "state", "product"),
                new CassandraCqlIncrementalStateUpdater<String, Number>());
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
