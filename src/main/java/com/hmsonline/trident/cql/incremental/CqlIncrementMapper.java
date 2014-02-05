package com.hmsonline.trident.cql.incremental;

import storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;

public interface CqlIncrementMapper<K, V> {

    public Statement read(K key);

    public Statement update(K key, V value, V oldValue);

    public V currentValue(ResultSet results);

    public K getKey(TridentTuple tuple);

    public V getValue(TridentTuple tuple);

}
