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

CREATE TABLE wordcounttable (
	source text,
	word text,
	count int,
	primary key(source, word)
);

CREATE TABLE multikeytable (
	k1 text,
	k2 text,
	v1 int,
	primary key (k1, k2)
);
