package com.hmsonline.trident.cql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

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

    @SuppressWarnings("serial")
    public static class Options<T> implements Serializable {
        public Serializer<T> serializer = null;
        public int localCacheSize = 5000;
        public String globalKey = "globalkey";
        public String tableName = "mytable";
        public Integer ttl = 86400; // 1 day
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory opaque(CqlClientFactory clientFactory) {
        Options<OpaqueValue> options = new Options<OpaqueValue>();
        return opaque(clientFactory, options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory opaque(CqlClientFactory clientFactory, Options<OpaqueValue> opts) {
        return new CassandraCqlMapStateFactory(clientFactory, StateType.OPAQUE, opts);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory transactional(CqlClientFactory clientFactory) {
        Options<TransactionalValue> options = new Options<TransactionalValue>();
        return transactional(clientFactory, options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory transactional(CqlClientFactory clientFactory, Options<TransactionalValue> opts) {
        return new CassandraCqlMapStateFactory(clientFactory, StateType.TRANSACTIONAL, opts);
    }

    public static StateFactory nonTransactional(CqlClientFactory clientFactory) {
        Options<Object> options = new Options<Object>();
        return nonTransactional(clientFactory, options);
    }

    public static StateFactory nonTransactional(CqlClientFactory clientFactory, Options<Object> opts) {
        return new CassandraCqlMapStateFactory(clientFactory, StateType.NON_TRANSACTIONAL, opts);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public CassandraCqlMapState(CqlClientFactory clientFactory, Options<T> options, Map conf) {
        this.options = options;
        this.serializer = options.serializer;
        this.clientFactory = clientFactory;
    }
    
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<T> values = new ArrayList<T>();
        // TODO: Use the mapper to get value from keys
        return values;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        // TODO: Use the mapper to put values into keys
    }
}
