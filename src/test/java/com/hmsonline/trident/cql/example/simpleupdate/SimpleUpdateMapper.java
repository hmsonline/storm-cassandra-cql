package com.hmsonline.trident.cql.example.simpleupdate;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import java.io.Serializable;

import org.apache.storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Update;
import com.hmsonline.trident.cql.mappers.CqlRowMapper;

public class SimpleUpdateMapper implements CqlRowMapper<Object, Object>, Serializable {
    private static final long serialVersionUID = 1L;

    public Statement map(TridentTuple tuple) {
        long t = System.currentTimeMillis() % 10;
        Update statement = update("mykeyspace", "mytable");
        statement.with(set("col1", tuple.getString(0))).where(eq("t", t));
        return statement;
    }

    public Statement map(TridentTuple tuple, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Statement retrieve(Object key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Statement map(Object key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object getValue(Row row) {
        // TODO Auto-generated method stub
        return null;
    }
}
