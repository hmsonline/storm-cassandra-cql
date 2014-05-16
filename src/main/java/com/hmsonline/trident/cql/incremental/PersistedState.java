package com.hmsonline.trident.cql.incremental;

public interface PersistedState<V> {
    V getValue();
    String getPartitionKey();
}
