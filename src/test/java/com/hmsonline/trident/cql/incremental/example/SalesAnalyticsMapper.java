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

    @Override
    public Statement read(String key) {
        Selection selection = QueryBuilder.select();
        selection.column("dimension").column("value");
        Select statement = selection.from("analytics");        
        Clause clause = QueryBuilder.eq("dimension", key.toString());
        statement.where(clause);
        return statement;
    }

    @Override
    public Statement update(String key, Number value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Number currentValue(ResultSet results) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getKey(TridentTuple tuple) {
        String state = tuple.getString(1);
        return "sum(price):" + state;
    }

    @Override
    public Number getValue(TridentTuple tuple) {
        return tuple.getInteger(0);
    }
}
