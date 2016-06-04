package com.hmsonline.trident.cql;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.task.IMetricsContext;

import com.datastax.driver.core.ConsistencyLevel;

@SuppressWarnings("rawtypes")
public class CassandraCqlStateFactory implements StateFactory {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlStateFactory.class);
    public static final String TRIDENT_CASSANDRA_MAX_BATCH_SIZE = "trident.cassandra.maxbatchsize";

    public static final int DEFAULT_MAX_BATCH_SIZE = 100;
    private static CqlClientFactory clientFactory;
    private ConsistencyLevel batchConsistencyLevel;

    public CassandraCqlStateFactory(ConsistencyLevel batchConsistencyLevel){
        this.batchConsistencyLevel = batchConsistencyLevel;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public State makeState(Map configuration, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        // worth synchronizing here?
        if (clientFactory == null) {
            clientFactory = new MapConfiguredCqlClientFactory(configuration);
        }
        final String maxBatchSizeString = (String) configuration.get(CassandraCqlStateFactory.TRIDENT_CASSANDRA_MAX_BATCH_SIZE);
        final int maxBatchSize = (maxBatchSizeString == null) ? DEFAULT_MAX_BATCH_SIZE : Integer.parseInt((String) maxBatchSizeString);
        LOG.debug("Creating State for partition [{}] of [{}]", new Object[]{partitionIndex, numPartitions});
        return new CassandraCqlState(clientFactory, maxBatchSize, batchConsistencyLevel);
    }
}
