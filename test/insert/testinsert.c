#include <dirent.h>
#include <postgresql/libpq-fe.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

void dec2bin(int decimal, char *binary) {
  int k = 0, n= 0;
  int remain;
  char temp[80];

  for(;k<33;) {
    remain = decimal % 2;
    decimal = decimal / 2;
    temp[k++] = remain + '0';
  }

  while (k>0)
    binary[n++] = temp[--k];

  binary[n-1] = 0;
}

int main( int argc, char *argv[] )
{
  PGresult *snapshot;
  PGconn *conn;
  char snapshot_name[16];
  char statpath[4096];
  struct stat st;
  PGresult *insert_result;
  char stat_query[1024];
  char binary[80];
  char abslink[10] = "false";
  struct dirent *ent;
  int ret = 0;
  DIR *dir;

  conn = PQsetdbLogin("localhost", "5432", NULL, NULL, "scratch", "treewalk", "testing");

  if (PQstatus(conn) != CONNECTION_OK) {
    fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
    PQfinish(conn);
    return -1;
  }
  
  snapshot = PQexec(conn, "SELECT name FROM current_snapshot WHERE ID = 1;");
    
  if (PQresultStatus(snapshot) != PGRES_TUPLES_OK) {
    fprintf(stderr, "SELECT current_snapshot command failed: %s\n", PQerrorMessage(conn));
    PQclear(snapshot);
    PQfinish(conn);
    return -1;
  }
    
  if (strncmp(PQgetvalue(snapshot, 0, 0), "snapshot1", 16) == 0)
    snprintf(snapshot_name, 16, "%s", "snapshot2");
  else if (strncmp(PQgetvalue(snapshot, 0, 0), "snapshot2", 16) == 0)
    snprintf(snapshot_name, 16, "%s", "snapshot1");

  if (!((strncmp(snapshot_name, "snapshot1", 16) == 0) || (strncmp(snapshot_name, "snapshot2", 16) == 0))) {
    fprintf(stderr, "could not get snapshot table from current_snapshot (got: %s)\n", PQgetvalue(snapshot, 0, 0));
    PQfinish(conn);
    return -1;
  }

  dir = opendir(argv[1]);
      
  if (!dir)
    perror("pstat.c, worker(), opendir()");
  else {
    while ((ent = readdir(dir)) != NULL) {
      if ((strncmp(ent->d_name, ".", 10)) && (strncmp(ent->d_name, "..", 10))) {
	strncpy(statpath, argv[1], 3071);
	strncat(statpath, "/", 1);
	strncat(statpath, ent->d_name, 1024);
	
	if (lstat(statpath, &st) == -1) {
	  fprintf(stderr, "%s\n", statpath);
	  perror("pstat.c, worker(), lstat()");
	  ret = 1;
	} 
	else { 
	  dec2bin(st.st_mode, binary);
	  if ((S_ISLNK(st.st_mode)) && (statpath[0] == '/')) {
	    bzero(abslink, 10);
	    snprintf(abslink, 5, "true");
	  }
	  else {
	    bzero(abslink, 10);
	    snprintf(abslink, 6, "false");          
	  }
	    
	  if (S_ISDIR(st.st_mode) && !(S_ISLNK(st.st_mode)))
	    snprintf(stat_query, 1024, "INSERT INTO %s (filename, inode, mode, nlink, uid, gid, size, block, block_size, atime, mtime, ctime, abslink) VALUES ('%s/', %ju, B'%s', %ju, %i, %i, %ju, %ju, %ju, timestamp without time zone 'epoch' + %ju * interval '1 second', timestamp without time zone 'epoch' + %ju * interval '1 second', timestamp without time zone 'epoch' + %ju * interval '1 second', %s);", snapshot_name, statpath, st.st_ino, binary, st.st_nlink, st.st_uid, st.st_gid, st.st_size, st.st_blocks, st.st_blksize, st.st_atime, st.st_mtime, st.st_ctime, abslink);
	  else
	    snprintf(stat_query, 1024, "INSERT INTO %s (filename, inode, mode, nlink, uid, gid, size, block, block_size, atime, mtime, ctime, abslink) VALUES ('%s', %ju, B'%s', %ju, %i, %i, %ju, %ju, %ju, timestamp without time zone 'epoch' + %ju * interval '1 second', timestamp without time zone 'epoch' + %ju * interval '1 second', timestamp without time zone 'epoch' + %ju * interval '1 second', %s);", snapshot_name, statpath, st.st_ino, binary, st.st_nlink, st.st_uid, st.st_gid, st.st_size, st.st_blocks, st.st_blksize, st.st_atime, st.st_mtime, st.st_ctime, abslink);
	    
	  printf("%s\n", stat_query);
	    
	  insert_result = PQexec(conn, stat_query);
	    
	  if (PQresultStatus(insert_result) != PGRES_COMMAND_OK) {
	    fprintf(stderr, "INSERT INTO snapshot command failed (%s): %s\n", statpath, PQerrorMessage(conn));
	    fprintf(stderr, "%s\n", stat_query);
	    ret =1;
	  }
	  PQclear(insert_result);
	}
      }
    }
  }

  if (strncmp(snapshot_name, "snapshot1", 16) == 0) {
    snapshot = PQexec(conn, "UPDATE current_snapshot SET name = 'snapshot1' WHERE ID = 1;");
    if (PQresultStatus(snapshot) != PGRES_COMMAND_OK) {
      fprintf(stderr, "UPDATE current_snapshot command failed: %s\n", PQerrorMessage(conn));
      PQclear(snapshot);
      PQfinish(conn);
      return -1;
    }
    PQclear(snapshot);
  } else {
    snapshot = PQexec(conn, "UPDATE current_snapshot SET name = 'snapshot2' WHERE ID = 1;");
    if (PQresultStatus(snapshot) != PGRES_COMMAND_OK) {
      fprintf(stderr, "UPDATE current_snapshot command failed: %s\n", PQerrorMessage(conn));
      PQclear(snapshot);
      PQfinish(conn);
      return -1;
    }
    PQclear(snapshot);
  }

  snapshot = PQexec(conn, "UPDATE current_snapshot SET updated = now() WHERE ID = 1;");
  if (PQresultStatus(snapshot) != PGRES_COMMAND_OK) {
    fprintf(stderr, "UPDATE current_snapshot,updated command failed: %s\n", PQerrorMessage(conn));
    PQclear(snapshot);
    PQfinish(conn);
    return -1;
  }

  PQclear(snapshot);

  PQfinish(conn);
  return ret;
}
