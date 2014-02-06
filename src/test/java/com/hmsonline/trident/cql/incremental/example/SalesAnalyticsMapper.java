package com.hmsonline.trident.cql.incremental.example;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.*;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.hmsonline.trident.cql.incremental.CqlIncrementMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;
import java.util.List;

public class SalesAnalyticsMapper implements CqlIncrementMapper<String, Number>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SalesAnalyticsMapper.class);


    // values assumed by the schema.cql; should make customizable by constructor
    public static final String KEYSPACE_NAME = "mykeyspace";
    public static final String TABLE_NAME = "incrementaltable";
    public static final String KEY_NAME = "k";
    public static final String VALUE_NAME = "v";

    @Override
    public Statement read(String key) {
        Selection selection = QueryBuilder.select();
        selection.column("v");
        Select statement = selection.from(KEYSPACE_NAME, TABLE_NAME);
        Clause clause = QueryBuilder.eq(KEY_NAME, key);
        statement.where(clause);
        return statement;
    }

    @Override
    public Statement update(String key, Number value, Number oldValue) {
        Update update = QueryBuilder.update(KEYSPACE_NAME, TABLE_NAME);
        Assignment assignment = QueryBuilder.set(VALUE_NAME, value);
        update.with(assignment);
        Clause clause = QueryBuilder.eq(KEY_NAME, key);
        update.where(clause);
        if (oldValue != null) {
            Clause conditionalClause = QueryBuilder.eq(VALUE_NAME, oldValue);
            update.onlyIf(conditionalClause);
        }
        return update;
    }

    @Override
    public Number currentValue(Row row) {
        return row.getInt(VALUE_NAME);
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
