package com.hmsonline.trident.cql;

import storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Statement;

/**
 * The <code>CqlTupleMapper</code> interface is responsible
 * for defining the structure of mapping and retrieving tuples
 * into the Cassandra store. 
 * <p>
 * The <code>CqlTupleMapper</code> also defines the methods of
 * getting and putting 
 * @author rlee
 *
 */
public interface CqlTupleMapper<K, V> {
	public Statement map(TridentTuple tuple);
	
	public Statement map(K key, V value);
	
	public Statement retrieve(K key);
	
	public V get(K key);
	
	public void put(K key, V value);
}
