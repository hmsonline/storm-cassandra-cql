package com.hmsonline.trident.cql.incremental;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.task.IMetricsContext;

import com.hmsonline.trident.cql.CqlClientFactory;
import com.hmsonline.trident.cql.MapConfiguredCqlClientFactory;

@SuppressWarnings("rawtypes")
// TODO: Is it worth subclassing from CassandraCqlStateFactory?
public class CassandraCqlIncrementalStateFactory<K, V> implements StateFactory {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlIncrementalStateFactory.class);
    private CqlClientFactory clientFactory;
    private CombinerAggregator<V> aggregator;
    private CqlIncrementMapper<K, V> mapper;

    public CassandraCqlIncrementalStateFactory(CombinerAggregator<V> aggregator, CqlIncrementMapper<K, V> mapper) {
        this.aggregator = aggregator;
        this.mapper = mapper;
    }
    
    protected void setCqlClientFactory(CqlClientFactory clientFactory){
        this.clientFactory = clientFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public State makeState(Map configuration, IMetricsContext metrics, int partitionIndex, int numPartitions) {        
        // NOTE: Lazy instantiation because Cluster is not serializable.
        if (clientFactory == null) {
            clientFactory = new MapConfiguredCqlClientFactory(configuration);
        }
        
        LOG.debug("Creating State for partition [{}] of [{}]", new Object[]{partitionIndex, numPartitions});
        return new CassandraCqlIncrementalState<K, V>(clientFactory, aggregator, mapper, partitionIndex);
    }
    
    
}
