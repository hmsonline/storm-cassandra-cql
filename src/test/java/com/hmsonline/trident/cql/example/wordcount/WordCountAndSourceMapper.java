package com.hmsonline.trident.cql.example.wordcount;

import java.io.Serializable;
import java.util.List;

import org.apache.storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.hmsonline.trident.cql.mappers.CqlRowMapper;

public class WordCountAndSourceMapper implements CqlRowMapper<List<String>, Number>, Serializable {
    private static final long serialVersionUID = 1L;
    //private static final Logger LOG = LoggerFactory.getLogger(WordCountAndSourceMapper.class);

    public static final String KEYSPACE_NAME = "mykeyspace";
    public static final String TABLE_NAME = "wordcounttable";
    public static final String SOURCE_KEY_NAME = "source";
    public static final String WORD_KEY_NAME = "word";
    public static final String VALUE_NAME = "count";

    @Override
    public Statement map(List<String> keys, Number value) {
        Insert statement = QueryBuilder.insertInto(KEYSPACE_NAME, TABLE_NAME);
        statement.value(WORD_KEY_NAME, keys.get(0));
        statement.value(SOURCE_KEY_NAME, keys.get(1));
        statement.value(VALUE_NAME, value);
        return statement;
    }

    @Override
    public Statement retrieve(List<String> keys) {
        // Retrieve all the columns associated with the keys
        Select statement = QueryBuilder.select().column(SOURCE_KEY_NAME)
                .column(WORD_KEY_NAME).column(VALUE_NAME)
                .from(KEYSPACE_NAME, TABLE_NAME);
        statement.where(QueryBuilder.eq(SOURCE_KEY_NAME, keys.get(0)));
        statement.where(QueryBuilder.eq(WORD_KEY_NAME, keys.get(1)));
        return statement;
    }

    @Override
    public Number getValue(Row row) {
        return (Number) row.getInt(VALUE_NAME);
    }

    @Override
    public Statement map(TridentTuple tuple) {
        return null;
    }


}
