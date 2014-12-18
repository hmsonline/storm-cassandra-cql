package com.hmsonline.trident.cql.incremental;

import backtype.storm.task.IMetricsContext;

import com.datastax.driver.core.ConsistencyLevel;
import com.hmsonline.trident.cql.CassandraCqlStateFactory;
import com.hmsonline.trident.cql.CqlClientFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.CombinerAggregator;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

@SuppressWarnings("rawtypes")
// TODO: Is it worth subclassing from CassandraCqlStateFactory?
public class CassandraCqlIncrementalStateFactory<K, V> implements StateFactory {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlIncrementalStateFactory.class);
    public static String TRIDENT_CASSANDRA_CQL_HOSTS = "trident.cassandra.cql.hosts";
    private static CqlClientFactory clientFactory;
    private CombinerAggregator<V> aggregator;
    private CqlIncrementMapper<K, V> mapper;
    private ConsistencyLevel batchConsistencyLevel;

    public CassandraCqlIncrementalStateFactory(CombinerAggregator<V> aggregator, CqlIncrementMapper<K, V> mapper,
            ConsistencyLevel batchConsistencyLevel) {
        this.aggregator = aggregator;
        this.mapper = mapper;
    }

    @Override
    public State makeState(Map configuration, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        // worth synchronizing here?
        if (clientFactory == null) {
            String hosts = (String) configuration.get(CassandraCqlStateFactory.TRIDENT_CASSANDRA_CQL_HOSTS);
            clientFactory = new CqlClientFactory(hosts, batchConsistencyLevel);
        }
        LOG.debug("Creating State for partition [{}] of [{}]", new Object[]{partitionIndex, numPartitions});
        return new CassandraCqlIncrementalState<K, V>(CassandraCqlIncrementalStateFactory.clientFactory, aggregator, mapper, partitionIndex);
    }
}
