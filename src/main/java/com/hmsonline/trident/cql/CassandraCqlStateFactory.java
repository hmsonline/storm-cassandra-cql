package com.hmsonline.trident.cql;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

import com.datastax.driver.core.ConsistencyLevel;

@SuppressWarnings("rawtypes")
public class CassandraCqlStateFactory implements StateFactory {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlStateFactory.class);
    public static final String TRIDENT_CASSANDRA_CQL_HOSTS = "trident.cassandra.cql.hosts";
    public static final String TRIDENT_CASSANDRA_CONSISTENCY = "trident.cassandra.consistency";
    public static final String TRIDENT_CASSANDRA_SERIAL_CONSISTENCY = "trident.cassandra.serial.consistency";
    public static final String TRIDENT_CASSANDRA_QUERY_TIMEOUT = "trident.cassandra.query.timeout";
    public static final String TRIDENT_CASSANDRA_CLUSTER_NAME = "trident.cassandra.cluster.name";    
    private static CqlClientFactory clientFactory;
    private ConsistencyLevel consistencyLevel;

    public CassandraCqlStateFactory(ConsistencyLevel consistencyLevel){
        this.consistencyLevel = consistencyLevel;
    }
    
    @Override
    public State makeState(Map configuration, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        // worth synchronizing here?
        if (clientFactory == null) {
            String hosts = (String) configuration.get(CassandraCqlStateFactory.TRIDENT_CASSANDRA_CQL_HOSTS);
            clientFactory = new CqlClientFactory(hosts, ConsistencyLevel.LOCAL_QUORUM);
        }
        LOG.debug("Creating State for partition [{}] of [{}]", new Object[]{partitionIndex, numPartitions});
        return new CassandraCqlState(CassandraCqlStateFactory.clientFactory, consistencyLevel);
    }
}
