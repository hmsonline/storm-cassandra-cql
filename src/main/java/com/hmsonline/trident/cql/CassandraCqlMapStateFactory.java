package com.hmsonline.trident.cql;

import java.util.Map;

import com.datastax.driver.core.Session;
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
import com.hmsonline.trident.cql.mappers.CqlRowMapper;

/**
 * The class responsible for generating instances of
 * the {@link CassandraCqlMapState}.
 *
 * @author robertlee
 */
public class CassandraCqlMapStateFactory implements StateFactory {
    private static final long serialVersionUID = 1L;

    private CqlClientFactory clientFactory;
    private StateType stateType;
    private Options<?> options;

    @SuppressWarnings("rawtypes")
    private CqlRowMapper mapper;

    @SuppressWarnings({"rawtypes"})
    public CassandraCqlMapStateFactory(CqlRowMapper mapper, StateType stateType, Options options) {
        this.stateType = stateType;
        this.options = options;
        this.mapper = mapper;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public State makeState(Map configuration, IMetricsContext metrics, int partitionIndex, int numPartitions) {

        if (clientFactory == null) {
            clientFactory = new CqlClientFactory(configuration);
        }

        Session session;
        if(options.keyspace != null) {
            session = clientFactory.getSession(options.keyspace);
        } else {
            session = clientFactory.getSession();
        }

        CassandraCqlMapState state = new CassandraCqlMapState(session, mapper, options, configuration);
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