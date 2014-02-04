Description
===================

This is a new CassandraState implementation built on the CQL java driver.  

Rationale
===================

For Cassandra, CQL has better support for lightweight transacations, batching, and collections.  
Also, CQL will likely get more attention than the legacy Thrift interface.  For these reasons, we decided to create
a C* state implementation built on CQL.
