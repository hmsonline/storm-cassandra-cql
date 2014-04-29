package com.hmsonline.trident.cql.incremental.example;

import com.hmsonline.trident.cql.incremental.PersistedState;

public class SalesState implements PersistedState<Number> {
    Number value;
    
    public SalesState(Number value){
        this.value = value;        
    }
    
    @Override
    public Number getValue() {
        return value;
    }    
}
