CREATE KEYSPACE mykeyspace
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE mykeyspace;

CREATE TABLE mytable (
    t int,
    col1 text,
    primary key (t)
);

CREATE TABLE incrementaltable (
    k text,
    v int,
    primary key (k)
);