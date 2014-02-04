package com.hmsonline.trident.cql.incremental.example;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.hmsonline.trident.cql.CassandraCqlStateFactory;
import com.hmsonline.trident.cql.CqlClientFactory;

/**
 * Created by bone on 1/28/14.
 */
public class ConditionalUpdate {

    /**
     * Let's see what happens when we perform a conditional update with stale data.
     */
    public static void main(){
        Map<String,String> configuration = new HashMap<String,String>();
        configuration.put(CassandraCqlStateFactory.TRIDENT_CASSANDRA_CQL_HOSTS, "localhost");
        CqlClientFactory clientFactory = new CqlClientFactory(configuration);

        Update statement = QueryBuilder.update("mykeyspace", "mytable");
        String key = "k";
        Integer value = 7;
        statement.with(QueryBuilder.set(key, value));
        statement.with(QueryBuilder.set(key, value));
        long t = System.currentTimeMillis() % 10;
        Clause clause = QueryBuilder.eq("t", t);
        statement.where(clause);


        clientFactory.getSession().execute("");

    }
}
