/* snapshots */

CREATE TABLE posix_snapshot1(
   filename TEXT NOT NULL PRIMARY KEY,
   inode INT NOT NULL,
   mode INT NOT NULL,
   nlink INT NOT NULL,
   uid INT NOT NULL,
   gid INT NOT NULL,
   size INT NOT NULL,
   atime TIMESTAMP NOT NULL,
   mtime TIMESTAMP NOT NULL,
   ctime TIMESTAMP NOT NULL,
   added TIMESTAMP DEFAULT NOW()
);
   CREATE INDEX posix_snapshot1_atime_index ON posix_snapshot1(atime);
   CREATE INDEX posix_snapshot1_mtime_index ON posix_snapshot1(mtime);
   CREATE INDEX posix_snapshot1_ctime_index ON posix_snapshot1(ctime);
   CREATE INDEX posix_snapshot1_added_index ON posix_snapshot1(added);

CREATE TABLE pan_snapshot1(
	filename TEXT NOT NULL PRIMARY KEY,
	total_num_compts INT NOT NULL,
	stripe_unit INT NOT NULL,
	parity_stripe_width INT NOT NULL,
	parity_stripe_depth INT NOT NULL,
	total_num_comps INT NOT NULL,
	stripe_unit INT NOT NULL
	added TIMESTAMP DEFAULT NOW()
);
   CREATE INDEX pan_snapshot1_added_index ON pan_snapshot1(added);


CREATE TABLE statvfs_snapshot1(
	filename TEXT NOT NULL PRIMARY KEY,
	f_bsize INT NOT NULL,
	f_frsize INT NOT NULL,
	f_blocks INT NOT NULL,
	f_bfree INT NOT NULL,
	f_bavail INT NOT NULL,
	f_files INT NOT NULL,
	f_ffree INT NOT NULL,
	f_favail INT NOT NULL,
	f_fsid INT NOT NULL,
	f_flag INT NOT NULL,
	f_namemax INT NOT NULL
	added TIMESTAMP DEFAULT NOW()
);
   CREATE INDEX statvfs_snapshot1_added_index ON statvfs_snapshot1(added);


CREATE TABLE pan_extended_snapshot1(
	filename TEXT NOT NULL PRIMARY KEY,
	mount_from_name TEXT NOT NULL,
	mount_from_name_len INT NOT NULL,
	volume_id INT NOT NULL,
	bladeset_id INT NOT NULL,
	bladeset_storageblade_count INT NOT NULL,
	bladeset_total_bytes INT NOT NULL,
	bladeset_free_bytes INT NOT NULL,
	bladeset_unreserved_free_bytes INT NOT NULL,
	bladeset_recon_spare_total_bytes INT NOT NULL,
	volume_live_bytes_used INT NOT NULL,
	volume_snapshot_bytes_used INT NOT NULL,
	volume_hard_quota_bytes INT NOT NULL,
	filler INT NOT NULL
	added TIMESTAMP DEFAULT NOW()
);
   CREATE INDEX pan_extended_snapshot1_added_index ON pan_extended_snapshot1(added);


/* second snapshots */

CREATE TABLE posix_snapshot2(
   filename TEXT NOT NULL PRIMARY KEY,
   inode INT NOT NULL,
   mode INT NOT NULL,
   nlink INT NOT NULL,
   uid INT NOT NULL,
   gid INT NOT NULL,
   size INT NOT NULL,
   atime TIMESTAMP NOT NULL,
   mtime TIMESTAMP NOT NULL,
   ctime TIMESTAMP NOT NULL,
   added TIMESTAMP DEFAULT NOW()
);
   CREATE INDEX posix_snapshot2_atime_index ON posix_snapshot2(atime);
   CREATE INDEX posix_snapshot2_mtime_index ON posix_snapshot2(mtime);
   CREATE INDEX posix_snapshot2_ctime_index ON posix_snapshot2(ctime);
   CREATE INDEX posix_snapshot2_added_index ON posix_snapshot2(added);

CREATE TABLE pan_snapshot2(
	filename TEXT NOT NULL PRIMARY KEY,
	total_num_compts INT NOT NULL,
	stripe_unit INT NOT NULL,
	parity_stripe_width INT NOT NULL,
	parity_stripe_depth INT NOT NULL,
	total_num_comps INT NOT NULL,
	stripe_unit INT NOT NULL
	added TIMESTAMP DEFAULT NOW()
);
   CREATE INDEX pan_snapshot2_added_index ON pan_snapshot2(added);


CREATE TABLE statvfs_snapshot2(
	filename TEXT NOT NULL PRIMARY KEY,
	f_bsize INT NOT NULL,
	f_frsize INT NOT NULL,
	f_blocks INT NOT NULL,
	f_bfree INT NOT NULL,
	f_bavail INT NOT NULL,
	f_files INT NOT NULL,
	f_ffree INT NOT NULL,
	f_favail INT NOT NULL,
	f_fsid INT NOT NULL,
	f_flag INT NOT NULL,
	f_namemax INT NOT NULL
	added TIMESTAMP DEFAULT NOW()
	
);
   CREATE INDEX statvfs_snapshot2_added_index ON statvfs_snapshot2(added);

CREATE TABLE pan_extended_snapshot2(
	filename TEXT NOT NULL PRIMARY KEY,
	mount_from_name TEXT NOT NULL,
	mount_from_name_len INT NOT NULL,
	volume_id INT NOT NULL,
	bladeset_id INT NOT NULL,
	bladeset_storageblade_count INT NOT NULL,
	bladeset_total_bytes INT NOT NULL,
	bladeset_free_bytes INT NOT NULL,
	bladeset_unreserved_free_bytes INT NOT NULL,
	bladeset_recon_spare_total_bytes INT NOT NULL,
	volume_live_bytes_used INT NOT NULL,
	volume_snapshot_bytes_used INT NOT NULL,
	volume_hard_quota_bytes INT NOT NULL,
	filler INT NOT NULL
	added TIMESTAMP DEFAULT NOW()
);   
	CREATE INDEX pan_extended_snapshot2_added_index ON pan_extended_snapshot2(added);
	
/* archives */

CREATE TABLE posix_archive(
   filename TEXT NOT NULL PRIMARY KEY,
   inode INT NOT NULL,
   mode INT NOT NULL,
   nlink INT NOT NULL,
   uid INT NOT NULL,
   gid INT NOT NULL,
   size INT NOT NULL,
   atime TIMESTAMP NOT NULL,
   mtime TIMESTAMP NOT NULL,
   ctime TIMESTAMP NOT NULL,
   added TIMESTAMP DEFAULT NOW()
);

CREATE TABLE pan_archive(
	filename TEXT NOT NULL PRIMARY KEY,
	total_num_compts INT NOT NULL,
	stripe_unit INT NOT NULL,
	parity_stripe_width INT NOT NULL,
	parity_stripe_depth INT NOT NULL,
	total_num_comps INT NOT NULL,
	stripe_unit INT NOT NULL
	added TIMESTAMP DEFAULT NOW()
);


CREATE TABLE statvfs_archive(
	filename TEXT NOT NULL PRIMARY KEY,
	f_bsize INT NOT NULL,
	f_frsize INT NOT NULL,
	f_blocks INT NOT NULL,
	f_bfree INT NOT NULL,
	f_bavail INT NOT NULL,
	f_files INT NOT NULL,
	f_ffree INT NOT NULL,
	f_favail INT NOT NULL,
	f_fsid INT NOT NULL,
	f_flag INT NOT NULL,
	f_namemax INT NOT NULL
	added TIMESTAMP DEFAULT NOW()
);


CREATE TABLE pan_extended_archive(
	filename TEXT NOT NULL PRIMARY KEY,
	mount_from_name TEXT NOT NULL,
	mount_from_name_len INT NOT NULL,
	volume_id INT NOT NULL,
	bladeset_id INT NOT NULL,
	bladeset_storageblade_count INT NOT NULL,
	bladeset_total_bytes INT NOT NULL,
	bladeset_free_bytes INT NOT NULL,
	bladeset_unreserved_free_bytes INT NOT NULL,
	bladeset_recon_spare_total_bytes INT NOT NULL,
	volume_live_bytes_used INT NOT NULL,
	volume_snapshot_bytes_used INT NOT NULL,
	volume_hard_quota_bytes INT NOT NULL,
	filler INT NOT NULL
	added TIMESTAMP DEFAULT NOW()
);
