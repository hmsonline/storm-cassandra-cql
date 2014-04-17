package com.hmsonline.trident.cql.incremental;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import storm.trident.tuple.TridentTuple;

public interface CqlIncrementMapper<K, V> {

    public Statement read(K key);

    public Statement update(K key, V value, V oldValue, Row readRow, long txid, int partitionIndex);

    public V currentValue(K key, Row row);

    public K getKey(TridentTuple tuple);

    public V getValue(TridentTuple tuple);
}
