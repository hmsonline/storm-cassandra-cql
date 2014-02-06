package com.hmsonline.trident.cql.example;

import java.io.Serializable;

import storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Statement;
import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import com.datastax.driver.core.querybuilder.Update;
import com.hmsonline.trident.cql.CqlTupleMapper;

public class ExampleMapper implements CqlTupleMapper, Serializable {
    private static final long serialVersionUID = 1L;

    public Statement map(TridentTuple tuple) {
        long t = System.currentTimeMillis() % 10;
        Update statement = update("mykeyspace", "mytable");
        statement.with(set("col1", tuple.getString(0))).where(eq("t", t));
        return statement;
    }
}
