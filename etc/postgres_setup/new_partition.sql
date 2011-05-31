-- for partitions be sure to use
SET constraint_exclusion = on;

CREATE TABLE archive_y2008m07 (
	CHECK ( added >= DATE '2008-07-01' AND added < DATE '2008-08-01')
) INHERITS (archive);

CREATE INDEX archive_y2008m07_added ON archive_y2008m07 (added);

CREATE OR REPLACE FUNCTION archive_insert_trigger()
RETURNS TRIGGER AS
$$
BEGIN
	INSERT INTO archive_y2008m07 VALUES (NEW.*);
	RETURN NULL;
END;		
$$
LANGUAGE plpgsql;

CREATE TRIGGER archive_insert_trigger
	   BEFORE INSERT ON archive
	   FOR EACH ROW EXECUTE PROCEDURE archive_insert_trigger();

-- INSERT INTO archive SELECT md5(filename), inode, uid, gid, size, atime, mtime, ctime FROM snapshot;
