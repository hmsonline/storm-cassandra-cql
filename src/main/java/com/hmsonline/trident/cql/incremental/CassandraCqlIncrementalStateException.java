package com.hmsonline.trident.cql.incremental;

public class CassandraCqlIncrementalStateException extends RuntimeException {
    private static final long serialVersionUID = 4532573980053416051L;

    public CassandraCqlIncrementalStateException(String message) {
        super(message);
    }
    
    public CassandraCqlIncrementalStateException(String message, Exception e) {
        super(message, e);
    }
}
