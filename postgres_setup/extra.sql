---------------- INSERTS --------------------
-- bulk insert into snapshot using a CSV file
copy snapshot (filename, inode, mode, nlink, uid, gid, size, block, block_size, 
    atime, mtime, ctime, abslink) 
    from '/path/to/file.csv' DELIMITERS '|' CSV; 

-- Insert data into the archive
INSERT INTO archive SELECT md5(filename), inode, mode, nlink, uid, gid, size, 
    block, block_size, atime, mtime, ctime, abslink FROM snapshot;

-- Update conversions table
INSERT INTO conversions SELECT filename, md5(filename) FROM snapshot WHERE NOT 
    EXISTS(select 1 from conversions where filename = snapshot.filename);

-- Isert into expired_files from snapshot
INSERT INTO expired_files (filename, uid, atime, mtime, ctime)
   SELECT filename, uid, atime, mtime, ctime
   FROM snapshot WHERE 
   atime <= now() - INTERVAL '1 week' AND
   ctime <= now() - INTERVAL '1 week' AND
   mtime <= now() - INTERVAL '1 week';

---------------- STATS --------------------

-- Show a list of all archive partitions (excluding archive)
SELECT table_name FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
   AND table_schema NOT IN ('pg_catalog', 'information_schema')
   AND table_name like 'archive_%';

-- Simple select from archive
SELECT uid, COUNT(*), avg(size), min(size), max(size) from archive GROUP BY uid;


-- Simple Histogram
SELECT bucket, (bucket-1)*1000 AS low, (bucket)*1000 AS high, COUNT(1) AS cnt
FROM
(
   SELECT WIDTH_BUCKET(size, 0, 75816, 10) AS bucket FROM archive
) AS derived_table
GROUP BY bucket ORDER BY bucket;


-- Histogram For time
                               
SELECT bucket, (bucket-1)*10 AS low, (bucket)*10 AS high, COUNT(1) AS cnt
FROM
(
   SELECT WIDTH_BUCKET(
       EXTRACT(EPOCH FROM (now() - atime))/(60*60*24), 0, 100, 10) 
   AS bucket FROM archive
) AS derived_table
GROUP BY bucket ORDER BY bucket;



-- More complex version of the histogram
CREATE OR REPLACE FUNCTION generate_size_histogram(refcursor) RETURNS SETOF refcursor AS
$$
DECLARE
   TOTAL_COUNT INTEGER;
   AVERAGE_VAL FLOAT;
   MAX_VAL INTEGER;
   MIN_VAL INTEGER;   
   STEP INTEGER;
   
   NUM_BUCKETS INTEGER;

   rowvar RECORD;


BEGIN
   RAISE NOTICE '--- Generating Histogram For Columns size ---';
   SELECT COUNT(*), AVG(size), MAX(size), MIN(size) 
      INTO TOTAL_COUNT, AVERAGE_VAL, MAX_VAL, MIN_VAL 
      FROM archive;
      
   RAISE NOTICE '--- THE COUNT IS % ---', TOTAL_COUNT;
   RAISE NOTICE '--- THE AVERAGE VAL IS % ---', AVERAGE_VAL;
   RAISE NOTICE '--- THE MAX VAL IS % ---', MAX_VAL;
   RAISE NOTICE '--- THE MIN VAL IS % ---', MIN_VAL;
   
   NUM_BUCKETS := CEILING(1 + 3.3 * log(TOTAL_COUNT));
   STEP := (MAX_VAL-MIN_VAL)/NUM_BUCKETS;
   
   RAISE NOTICE '--- Histogram Contains % Buckets ---', NUM_BUCKETS;
   RAISE NOTICE '--- Step Size is % ---', STEP;
   
   OPEN $1 for 
      SELECT bucket, MIN_VAL + (bucket-1)*STEP AS low, MIN_VAL + bucket*STEP AS high, COUNT(1) AS cnt
      FROM
      (
         SELECT width_bucket(size, MIN_VAL, MAX_VAL+1, NUM_BUCKETS) AS bucket FROM archive
      ) AS derived_table
      GROUP BY bucket ORDER BY bucket;
   
   RETURN NEXT $1;

END;
$$
LANGUAGE plpgsql;


-- How to create the hisogram based using the defined function
BEGIN;
SELECT * FROM generate_size_histogram('a');
FETCH ALL FROM a;
COMMIT;


-- Useful resource usage query

SELECT
tablename,
pg_size_pretty(pg_total_relation_size(tablename)) AS total_usage,
pg_size_pretty((pg_total_relation_size(tablename)
   - pg_relation_size(tablename))) AS external_table_usage
FROM pg_tables
WHERE schemaname != 'pg_catalog'
   AND schemaname != 'information_schema'
ORDER BY pg_total_relation_size(tablename);


--  # Python Octal to Binary 
--  def int2bin(n, count=24):
--      return "".join([str((n >> y) & 1) for y in range(count-1, -1, -1)])
--
--  int2bin(int("0100000", 8), 32)
--
--  # => '00000000000000001000000000000000'
--
--


CREATE OR REPLACE VIEW file_paths as 
SELECT 
    (regexp_matches(filename, E'(.*/)(.*?\.?.*)'))[1] as filepath,
    (regexp_matches(filename, E'(.*/)(.*?\.?.*)'))[2] as filename
FROM snapshot
WHERE mode & b'00000000000000001000000000000000' = b'00000000000000001000000000000000';

SELECT filepath, COUNT(*) FROM file_paths 
GROUP by filepath order by filepath;

   
