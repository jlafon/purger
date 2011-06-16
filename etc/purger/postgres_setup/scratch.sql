DROP DATABASE scratch;
CREATE DATABASE scratch;
ALTER DATABASE scratch OWNER TO treewalk;

\c scratch;

CREATE LANGUAGE plpgsql;

-- This table holds a snapshot of the file system
CREATE TABLE snapshot1(
   filename TEXT NOT NULL PRIMARY KEY,
   parent TEXT NOT NULL,
   inode BIGINT NOT NULL,
   mode BIT(32) NOT NULL,
   nlink INT NOT NULL,
   uid INT NOT NULL,
   gid INT NOT NULL,
   size BIGINT NOT NULL,
   block BIGINT NOT NULL,
   block_size INT NOT NULL,
   atime TIMESTAMP NOT NULL,
   mtime TIMESTAMP NOT NULL,
   ctime TIMESTAMP NOT NULL,
   abslink BOOLEAN,
   added TIMESTAMP DEFAULT NOW()
);
   CREATE INDEX snapshot1_parent_index ON snapshot1(parent);
   CREATE INDEX snapshot1_atime_index ON snapshot1(atime);
   CREATE INDEX snapshot1_mtime_index ON snapshot1(mtime);
   CREATE INDEX snapshot1_ctime_index ON snapshot1(ctime);
   
-- A second identical database, these two will swap using current_snapshot
CREATE TABLE snapshot2(
   filename TEXT NOT NULL PRIMARY KEY,
   parent TEXT NOT NULL,
   inode BIGINT NOT NULL,
   mode BIT(32) NOT NULL,
   nlink INT NOT NULL,
   uid INT NOT NULL,
   gid INT NOT NULL,
   size BIGINT NOT NULL,
   block BIGINT NOT NULL,
   block_size INT NOT NULL,
   atime TIMESTAMP NOT NULL,
   mtime TIMESTAMP NOT NULL,
   ctime TIMESTAMP NOT NULL,
   abslink BOOLEAN,
   added TIMESTAMP DEFAULT NOW()
);
   CREATE INDEX snapshot2_parent_index ON snapshot2(parent);
   CREATE INDEX snapshot2_atime_index ON snapshot2(atime);
   CREATE INDEX snapshot2_mtime_index ON snapshot2(mtime);
   CREATE INDEX snapshot2_ctime_index ON snapshot2(ctime);

   
-- Holds the name of the snapshot database that should currently be used
CREATE TABLE current_snapshot(
   id INT DEFAULT(1),
   name TEXT NOT NULL DEFAULT('snapshot2'),
   updated TIMESTAMP DEFAULT NOW()
);

-- Default this to 'snapshot1'
INSERT INTO current_snapshot(name) values(default);

-- A partitioned archive which will hold historical data taken from the snapshot
CREATE TABLE archive(
   md5 TEXT NOT NULL,
   inode BIGINT NOT NULL,
   mode BIT(32) NOT NULL,
   nlink INT NOT NULL,
   uid INT NOT NULL,
   gid INT NOT NULL,
   size BIGINT NOT NULL,
   block BIGINT NOT NULL,
   block_size INT NOT NULL,
   atime TIMESTAMP NOT NULL,
   mtime TIMESTAMP NOT NULL,
   ctime TIMESTAMP NOT NULL,
   abslink BOOLEAN,
   added TIMESTAMP DEFAULT NOW()
);


-- This simply holds the last month we created a partition to determine if we should create another;
CREATE TABLE recent_month(
   id INT DEFAULT(1),
   recent DATE NOT NULL
);

INSERT INTO recent_month(recent) values(current_date);
      
-- Holds the md5 conversions for efficient lookups
CREATE TABLE conversions(
   filename TEXT NOT NULL,
   md5       TEXT NOT NULL PRIMARY KEY
);


-- Creates the basic expired_files table type
CREATE TABLE expired_files(
   filename TEXT NOT NULL PRIMARY KEY,
   parent TEXT NOT NULL,
   uid INT NOT NULL,
   atime TIMESTAMP NOT NULL,
   mtime TIMESTAMP NOT NULL,
   ctime TIMESTAMP NOT NULL,
   added TIMESTAMP DEFAULT NOW(),
   warned BOOL DEFAULT false, 
   -- check constraints to be sure files that
   -- are entered are defintiely 'expired'
   check (atime <= now() - INTERVAL '1 week'),
   check (mtime <= now() - INTERVAL '1 week'),
   check (ctime <= now() - INTERVAL '1 week')
);

CREATE INDEX expired_files_uid_index ON expired_files(uid);

-- Set permissions on tables
GRANT ALL PRIVILEGES ON archive TO treewalk;
GRANT ALL PRIVILEGES ON conversions TO treewalk;
GRANT ALL PRIVILEGES ON current_snapshot TO treewalk;
GRANT ALL PRIVILEGES ON expired_files TO treewalk;
GRANT ALL PRIVILEGES ON recent_month TO treewalk;
GRANT ALL PRIVILEGES ON snapshot1 TO treewalk;
GRANT ALL PRIVILEGES ON snapshot2 TO treewalk;
ALTER TABLE snapshot1 OWNER TO treewalk;
ALTER TABLE snapshot2 OWNER TO treewalk;

-- Create the function warningtest which is called by the trigger
CREATE OR REPLACE FUNCTION warningtest() RETURNS trigger AS 
$$
    BEGIN
        IF NEW.atime != OLD.atime OR NEW.mtime != OLD.mtime OR NEW.ctime != OLD.ctime THEN
         NEW.warned := FALSE;
      END IF;
        RETURN NEW;
    END;
$$ 
LANGUAGE plpgsql;


-- Create the warning test trigger
CREATE TRIGGER warningtest BEFORE UPDATE ON expired_files
   FOR EACH ROW EXECUTE PROCEDURE warningtest();
   
-- This function attempts to do an update, but if that fails in inserts instead
CREATE OR REPLACE FUNCTION merge(filenamev TEXT, uidv INT, atimev TIMESTAMP, 
    mtimev TIMESTAMP, ctimev TIMESTAMP) RETURNS void AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE expired_files SET 
           uid = uidv,
           atime = atimev,
           mtime =  mtimev,
           ctime = ctimev
        WHERE filename = filenamev;
        IF found THEN
           RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO expired_files (filename, uid, atime, mtime, ctime)
            VALUES  (filenamev, uidv, atimev, mtimev, ctimev);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- do nothing, and loop to try the UPDATE again
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

