package com.hmsonline.trident.cql;

import java.util.HashMap;
import java.util.Map;

import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import com.hmsonline.trident.cql.CassandraCqlMapState.Options;

/**
 * The class responsible for generating instances of
 * the {@link CassandraCqlMapState}. 
 * 
 * @author robertlee
 */
public class CassandraCqlMapStateFactory implements StateFactory {
	private static final long serialVersionUID = 1L;
	private CqlClientFactory clientFactory;
	private CqlTupleMapper mapper;
	private StateType stateType;
    private Options<?> options;
    
    @SuppressWarnings("rawtypes")
	private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = new HashMap<StateType, Serializer>();
    static {
        DEFAULT_SERIALZERS.put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public CassandraCqlMapStateFactory(CqlTupleMapper mapper, StateType stateType, Options options) {
        this.stateType = stateType;
        this.options = options;
        this.mapper = mapper;

        if (this.options.serializer == null) {
            this.options.serializer = DEFAULT_SERIALZERS.get(stateType);
        }

        if (this.options.serializer == null) {
            throw new RuntimeException("Serializer should be specified for type: " + stateType);
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public State makeState(Map configuration, IMetricsContext metrics, int partitionIndex, int numPartitions) {
    	// worth synchronizing here?
    	if (clientFactory == null){
    		clientFactory = new CqlClientFactory(configuration);
    	}
    	
        CassandraCqlMapState state = new CassandraCqlMapState(clientFactory, mapper, options, configuration);
        state.registerMetrics(configuration, metrics);
        
        CachedMap cachedMap = new CachedMap(state, options.localCacheSize);

        MapState mapState;
        if (stateType == StateType.NON_TRANSACTIONAL) {
            mapState = NonTransactionalMap.build(cachedMap);
        } else if (stateType == StateType.OPAQUE) {
            mapState = OpaqueMap.build(cachedMap);
        } else if (stateType == StateType.TRANSACTIONAL) {
            mapState = TransactionalMap.build(cachedMap);
        } else {
            throw new RuntimeException("Unknown state type: " + stateType);
        }

        return new SnapshottableMap(mapState, new Values(options.globalKey));
    }

}