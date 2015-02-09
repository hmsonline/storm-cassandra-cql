package com.hmsonline.trident.cql.example.incremental;

import java.util.HashMap;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Sum;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.hmsonline.trident.cql.CqlClientFactory;
import com.hmsonline.trident.cql.CqlUnitClientFactory;
import com.hmsonline.trident.cql.MapConfiguredCqlClientFactory;
import com.hmsonline.trident.cql.incremental.CassandraCqlIncrementalStateFactory;
import com.hmsonline.trident.cql.incremental.CassandraCqlIncrementalStateUpdater;

public class SalesAnalyticsTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SalesAnalyticsTopology.class);
    @Rule
    public static CassandraCQLUnit cqlUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("schema.cql","mykeyspace"));

    public static StormTopology buildTopology() {
        LOG.info("Building topology.");
        TridentTopology topology = new TridentTopology();
        SalesSpout spout = new SalesSpout();
        Stream inputStream = topology.newStream("sales", spout);
        SalesAnalyticsMapper mapper = new SalesAnalyticsMapper();
        CqlClientFactory clientFactory = new CqlUnitClientFactory(new HashMap<Object, Object>(), cqlUnit);
        inputStream.partitionPersist(
                new CassandraCqlIncrementalStateFactory<String, Number>(new Sum(), mapper, clientFactory),
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
