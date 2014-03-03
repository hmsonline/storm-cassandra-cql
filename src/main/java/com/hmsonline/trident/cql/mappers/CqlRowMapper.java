package com.hmsonline.trident.cql.mappers;

import com.datastax.driver.core.Row;

/**
 * The <code>CqlRowMapper</code> interface extends <code>CqlTupleMapper</code> 
 * with the ability to translate a single row into an object.
 *
 * @param K the key to map and retrieve
 * @param V the value to map and retrieve
 * @author boneill
 */
public abstract interface CqlRowMapper<K, V> extends CqlTupleMapper<K,V> {
    public V getValue(Row row);
}
