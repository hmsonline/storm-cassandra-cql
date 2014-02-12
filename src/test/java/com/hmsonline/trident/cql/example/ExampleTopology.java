package com.hmsonline.trident.cql.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.hmsonline.trident.cql.CassandraCqlMapState;
import com.hmsonline.trident.cql.CassandraCqlMapStateFactory;
import com.hmsonline.trident.cql.CassandraCqlStateFactory;
import com.hmsonline.trident.cql.CassandraCqlStateUpdater;
import com.hmsonline.trident.cql.example.wordcount.WordCountMapper;

public class ExampleTopology {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleTopology.class);

    public static StormTopology buildTopology() {
        LOG.info("Building topology.");
        TridentTopology topology = new TridentTopology();
//        ExampleSpout spout = new ExampleSpout();
//        Stream inputStream = topology.newStream("test", spout);
//        ExampleMapper mapper = new ExampleMapper();
        //inputStream.partitionPersist(new CassandraCqlStateFactory(), new Fields("test"), new CassandraCqlStateUpdater(mapper));
        
        // Testing the map state
        @SuppressWarnings("unchecked")
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        spout.setCycle(true);
        
        TridentState wordCounts =
        	     topology.newStream("spout1", spout)
        	       .each(new Fields("sentence"), new Split(), new Fields("word"))
        	       .groupBy(new Fields("word"))
        	       .persistentAggregate(CassandraCqlMapState.opaque(new ExampleMapper()), 
        	    		   new Count(), new Fields("count"))
        	       .parallelismHint(6);
        
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
