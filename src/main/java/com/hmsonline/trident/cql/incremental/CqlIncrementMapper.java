package com.hmsonline.trident.cql.incremental;

import java.util.List;

import org.apache.storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;

public interface CqlIncrementMapper<K, V> {

    public Statement read(K key);

    public Statement update(K key, V value, PersistedState<V> state, long txid, int partitionIndex);

    public PersistedState<V> currentState(K key, List<Row> rows);

    public K getKey(TridentTuple tuple);

    public V getValue(TridentTuple tuple);
}
