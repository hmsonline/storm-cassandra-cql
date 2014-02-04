package com.hmsonline.trident.cql.incremental.example;

import java.io.Serializable;

import storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.hmsonline.trident.cql.incremental.CqlIncrementMapper;

public class SalesAnalyticsMapper implements CqlIncrementMapper<String, Number>, Serializable {
    private static final long serialVersionUID = 1L;

    // values assumed by the schema.cql; should make customizable by constructor
    private static final String KEYSPACE_NAME = "mykeyspace";
    private static final String TABLE_NAME = "incrementaltable";

    @Override
    public Statement read(String key) {
        Selection selection = QueryBuilder.select();
        selection.column("v");
        Select statement = selection.from(KEYSPACE_NAME, TABLE_NAME);        
        Clause clause = QueryBuilder.eq("k", key.toString());
        statement.where(clause);
        return statement;
    }

    @Override
    public Statement update(String key, Number value) {
        Update update = QueryBuilder.update(KEYSPACE_NAME, TABLE_NAME);
        Assignment assignment = QueryBuilder.set("v", value);
        update.with(assignment);
        Clause clause = QueryBuilder.eq("k", key.toString());
        update.where(clause);
        return update;
    }

    @Override
    public Number currentValue(ResultSet results) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getKey(TridentTuple tuple) {
        String state = tuple.getString(1);
        return state;
    }

    @Override
    public Number getValue(TridentTuple tuple) {
        return tuple.getInteger(0);
    }
}
