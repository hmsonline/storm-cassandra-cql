package com.hmsonline.trident.cql;

import storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Statement;

public interface CqlTupleMapper {
	public Statement map(TridentTuple tuple);
}
