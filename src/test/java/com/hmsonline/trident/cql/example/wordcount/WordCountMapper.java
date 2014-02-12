package com.hmsonline.trident.cql.example.wordcount;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.hmsonline.trident.cql.CqlClientFactory;
import com.hmsonline.trident.cql.CqlTupleMapper;

public class WordCountMapper implements CqlTupleMapper<String, Number>, Serializable {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(WordCountMapper.class);
    
    public static final String KEYSPACE_NAME = "mykeyspace";
    public static final String TABLE_NAME = "wordcounttable";
    public static final String KEY_NAME = "word";
    public static final String VALUE_NAME = "count";
    
	@Override
	public Statement map(String word, Number count) {		
        Update statement = QueryBuilder.update(KEYSPACE_NAME, TABLE_NAME);
        statement.with(QueryBuilder.set(KEY_NAME, word)).where(QueryBuilder.eq(VALUE_NAME, count));
        
        return statement;
	}

	@Override
	public Statement retrieve(String word) {
		Select statement = QueryBuilder.select().column(KEY_NAME).from(KEYSPACE_NAME, TABLE_NAME);
        statement.where(QueryBuilder.eq(KEY_NAME, word));
        return statement;
	}

	@Override
	public Number get(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void put(String word, Number count) {
		LOG.debug("Putting the key,value pair: [{},{}]", word, count);
		
        Insert statement = QueryBuilder.insertInto(KEYSPACE_NAME, TABLE_NAME);
        statement.value(word, count);		
	}

	@Override
	public Statement map(TridentTuple tuple) {
		// TODO Auto-generated method stub
		return null;
	}

	
}
