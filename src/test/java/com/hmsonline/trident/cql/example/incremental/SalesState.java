package com.hmsonline.trident.cql.example.incremental;

import com.hmsonline.trident.cql.incremental.PersistedState;

public class SalesState implements PersistedState<Number> {
    Number value;
    String partitionsKey;
    public SalesState(Number value,String partitionsKey){
        this.value = value;
        this.partitionsKey = partitionsKey;
    }
    
    @Override
    public Number getValue() {
        return value;
    }

    @Override
    public String getPartitionKey() {
        return partitionsKey;
    }
}
