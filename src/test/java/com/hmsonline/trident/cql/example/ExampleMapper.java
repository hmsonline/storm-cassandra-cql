package com.hmsonline.trident.cql.example;

import java.io.Serializable;

import storm.trident.tuple.TridentTuple;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.hmsonline.trident.cql.CqlTupleMapper;

public class ExampleMapper implements CqlTupleMapper, Serializable {
    private static final long serialVersionUID = 1L;

    public Statement map(TridentTuple tuple) {
        Update statement = QueryBuilder.update("mykeyspace", "mytable");
        String field = "col1";
        String value = tuple.getString(0);
        Assignment assignment = QueryBuilder.set(field, value);
        statement.with(assignment);
        long t = System.currentTimeMillis() % 10;
        Clause clause = QueryBuilder.eq("t", t);
        statement.where(clause);
        return statement;
    }
}
