package com.hmsonline.trident.cql.incremental.example;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;

import java.io.Serializable;

import storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.hmsonline.trident.cql.incremental.CqlIncrementMapper;

public class SalesAnalyticsMapper implements CqlIncrementMapper<String, Number>, Serializable {
    private static final long serialVersionUID = 1L;
    //private static final Logger LOG = LoggerFactory.getLogger(SalesAnalyticsMapper.class);

    // values assumed by the schema.cql; should make customizable by constructor
    public static final String KEYSPACE_NAME = "mykeyspace";
    public static final String TABLE_NAME = "incrementaltable";
    public static final String KEY_NAME = "k";
    public static final String VALUE_NAME = "v";

    @Override
    public Statement read(String key) {
        Select statement = select().column("v").from(KEYSPACE_NAME, TABLE_NAME);
        statement.where(eq(KEY_NAME, key));
        return statement;
    }

    @Override
    public Statement update(String key, Number value, Number oldValue, long txid, int partition) {
        Update update = QueryBuilder.update(KEYSPACE_NAME, TABLE_NAME);
        update.with(set(VALUE_NAME, value)).where(eq(KEY_NAME, key));
        if (oldValue != null) {
            update.onlyIf(eq(VALUE_NAME, oldValue));
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
