package com.hmsonline.trident.cql;

import backtype.storm.task.IMetricsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

@SuppressWarnings("rawtypes")
public class CassandraCqlStateFactory implements StateFactory {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlStateFactory.class);
    public static String TRIDENT_CASSANDRA_CQL_HOSTS = "trident.cassandra.cql.hosts";
    private static CqlClientFactory clientFactory;

    @Override
    public State makeState(Map configuration, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        // worth synchronizing here?
        if (clientFactory == null) {
            clientFactory = new CqlClientFactory(configuration);
        }
        LOG.debug("Creating State for partition [{}] of [{}]", new Object[]{partitionIndex, numPartitions});
        return new CassandraCqlState(CassandraCqlStateFactory.clientFactory);
    }
}
