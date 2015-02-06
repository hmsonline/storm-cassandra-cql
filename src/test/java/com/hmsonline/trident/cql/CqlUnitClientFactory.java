package com.hmsonline.trident.cql;

import java.util.Map;

import org.cassandraunit.CassandraCQLUnit;

import com.datastax.driver.core.Session;

public class CqlUnitClientFactory extends MapConfiguredCqlClientFactory {
    private static final long serialVersionUID = 1L;
    public CassandraCQLUnit cqlUnit;
    
    public CqlUnitClientFactory(final Map<Object,Object> configuration, CassandraCQLUnit cqlUnit) {
        super(configuration);
        this.cqlUnit = cqlUnit;
    }
    
    @Override
    public Session getSession(){
        if (cqlUnit.session == null)
            throw new RuntimeException("No session established in CQL_UNIT");
        return cqlUnit.session;
    }
}
