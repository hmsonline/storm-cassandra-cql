package com.hmsonline.trident.cql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.BatchStatement.Type;
import com.google.common.collect.ImmutableMap;

import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;

/**
 * 
 * 
 * @author robertlee
 *
 * @param <T> The generic state to back
 */
public class CassandraCqlMapState<T> implements IBackingMap<T> {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlMapState.class);

    private Options<T> options;
    private Serializer<T> serializer;
    private CqlClientFactory clientFactory;
    private CqlTupleMapper mapper;
    
    // Metrics for storm metrics registering
    CountMetric _mreads;
    CountMetric _mwrites;
    CountMetric _mexceptions;

    @SuppressWarnings("serial")
    public static class Options<T> implements Serializable {
        public Serializer<T> serializer = null;
        public int localCacheSize = 5000;
        public String globalKey = "globalkey";
        public String tableName = "mytable";
        public Integer ttl = 86400; // 1 day
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory opaque(CqlTupleMapper mapper) {
        Options<OpaqueValue> options = new Options<OpaqueValue>();
        return opaque(mapper, options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory opaque(CqlTupleMapper mapper, Options<OpaqueValue> opts) {
        return new CassandraCqlMapStateFactory(mapper, StateType.OPAQUE, opts);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory transactional(CqlTupleMapper mapper) {
        Options<TransactionalValue> options = new Options<TransactionalValue>();
        return transactional(mapper, options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory transactional(CqlTupleMapper mapper, Options<TransactionalValue> opts) {
        return new CassandraCqlMapStateFactory(mapper, StateType.TRANSACTIONAL, opts);
    }

    public static StateFactory nonTransactional(CqlTupleMapper mapper) {
        Options<Object> options = new Options<Object>();
        return nonTransactional(mapper, options);
    }

    public static StateFactory nonTransactional(CqlTupleMapper mapper, Options<Object> opts) {
        return new CassandraCqlMapStateFactory(mapper, StateType.NON_TRANSACTIONAL, opts);
    }

    @SuppressWarnings({ "rawtypes" })
    public CassandraCqlMapState(CqlClientFactory clientFactory, CqlTupleMapper mapper, Options<T> options, Map conf) {
        this.options = options;
        this.serializer = options.serializer;
        this.clientFactory = clientFactory;
        this.mapper = mapper;
    }
    
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
    	//LOG.debug("Retrieving the following keys: {}", keys);
        List<T> values = new ArrayList<T>(keys.size());

        for (List<Object> rowKey : keys) {
        	for (Object key : rowKey ) {
        		//ResultSet result = clientFactory.getSession().execute(mapper.retrieve(key));
        		values.add((T) new Long(0));
        	}
        }
		
        return values;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
    	//LOG.debug("Putting the following keys: {} with values: {}", keys, values);
    	
    	Iterator<T> iter = values.iterator();
        for (List<Object> rowKey : keys) {
        	for (Object key : rowKey ) {
        		mapper.put(key, iter.next());
        	}
        }
    }
    
    public void registerMetrics(Map conf, IMetricsContext context) {
        int bucketSize = (Integer) (conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
        _mreads = context.registerMetric("memcached/readCount", new CountMetric(), bucketSize);
        _mwrites = context.registerMetric("memcached/writeCount", new CountMetric(), bucketSize);
        _mexceptions = context.registerMetric("memcached/exceptionCount", new CountMetric(), bucketSize);
      }
}
