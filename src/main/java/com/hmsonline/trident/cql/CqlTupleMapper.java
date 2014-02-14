package com.hmsonline.trident.cql;

import storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;

/**
 * The <code>CqlTupleMapper</code> interface is responsible
 * for defining the structure of mapping and retrieving tuples
 * into the Cassandra store. 
 * 
 * @param K the key to map and retrieve
 * @param V the value to map and retrieve
 * 
 * @author rlee
 */
public interface CqlTupleMapper<K, V> {	
	public Statement map(K key, V value);
	
	public Statement map(TridentTuple tuple);
	
	public Statement retrieve(K key);
	
	public V getValue(Row row);	
}
