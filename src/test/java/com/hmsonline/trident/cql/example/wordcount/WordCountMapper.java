package com.hmsonline.trident.cql.example.wordcount;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.hmsonline.trident.cql.CqlTupleMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;
import java.util.List;

public class WordCountMapper implements CqlTupleMapper<List<String>, Number>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(WordCountMapper.class);

    public static final String KEYSPACE_NAME = "mykeyspace";
    public static final String TABLE_NAME = "wordcounttable";
    public static final String KEY_NAME = "word";
    public static final String VALUE_NAME = "count";

    @Override
    public Statement map(List<String> keys, Number value) {
        Insert statement = QueryBuilder.insertInto(KEYSPACE_NAME, TABLE_NAME);
        statement.value(KEY_NAME, keys.get(0));
        statement.value(VALUE_NAME, value);
        return statement;
    }

    @Override
    public Statement retrieve(List<String> keys) {
        // Retrieve all the columns associated with the keys
        Select statement = QueryBuilder.select().column(KEY_NAME).column(VALUE_NAME).from(KEYSPACE_NAME, TABLE_NAME);
        statement.where(QueryBuilder.eq(KEY_NAME, keys.get(0)));
        return statement;
    }

    @Override
    public Number getValue(Row row) {
        return (Number) row.getInt(VALUE_NAME);
    }

    @Override
    public Statement map(TridentTuple tuple) {
        // TODO Auto-generated method stub
        return null;
    }


}
