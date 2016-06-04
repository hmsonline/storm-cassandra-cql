package com.hmsonline.trident.cql.example.wordcount;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;


public class IntegerCount implements CombinerAggregator<Integer> {
    private static final long serialVersionUID = 1L;

    @Override
    public Integer init(TridentTuple tuple) {
        return 1;
    }

    @Override
    public Integer combine(Integer val1, Integer val2) {
        return val1 + val2;
    }

    @Override
    public Integer zero() {
        return 0;
    }

}
