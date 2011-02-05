#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <postgresql/libpq-fe.h>
#include <sys/statfs.h>
#include <pwd.h>
#include <grp.h>
#include <getopt.h>

#define CFG_FILE "db.conf"

enum qtype {
  SIZE = 1,
  NFILES,
  USAGE,
  GROUP
};

struct dbinfo_t {
  char host[256];
  char port[16];
  char user[256];
  char pass[256];
};
typedef struct dbinfo_t dbinfo_t;

int printstat(const char* filesystem) {
  struct statfs st;
  char path[1024];

  snprintf(path, 1024, "/panfs/%s", filesystem);
  if (statfs(path, &st) == -1)
    perror("ERROR stat-ing filesystem");
  
  printf("%s filesystem:\n", path);
  printf("    Filesystem size is (%.0f Gbytes)\n", ((st.f_blocks * st.f_bsize)/(1024.0 * 1024.0 * 1024.0)));
  printf("    Current space available is (%.0f Gbytes) (%.0f%%)\n", ((st.f_bavail * st.f_bsize)/(1024.0 * 1024.0 * 1024.0)), (((double)st.f_bavail / (double)st.f_blocks) * 100.0));

  return ((st.f_blocks * st.f_bsize)/(1024 * 1024 * 1024));
}

int printusage(const char* filesystem, int count, int gb, int query_type, dbinfo_t dbinfo) {
  PGconn *conn;
  PGresult *snapshot_res;
  PGresult *query_res;
  char snapshot[30];
  char query[80];
  int results, i;
  struct passwd *pw;
  struct group *gr;

  conn = PQsetdbLogin(dbinfo.host, dbinfo.port, NULL, NULL, filesystem, dbinfo.user, dbinfo.pass);
  if (PQstatus(conn) != CONNECTION_OK) {
    fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
    PQfinish(conn);
    return -1;
  }
  
  snapshot_res = PQexec(conn, "SELECT name FROM current_snapshot WHERE ID = 1;");
  if (PQresultStatus(snapshot_res) != PGRES_TUPLES_OK) {
    fprintf(stderr, "SELECT current_snapshot command failed: %s\n", PQerrorMessage(conn));
    PQclear(snapshot_res);
    return -1;
  }
  
  if (strncmp(PQgetvalue(snapshot_res, 0, 0), "snapshot", 10) == 0)
    snprintf(snapshot, 30, "snapshot2");
  else
    snprintf(snapshot, 30, "snapshot");

  PQclear(snapshot_res);
  
  if (query_type == SIZE)
    snprintf(query, 80, "SELECT uid,sum(size) FROM %s GROUP BY uid ORDER BY sum DESC;", snapshot);
  else if (query_type == NFILES)
    snprintf(query, 80, "SELECT uid,count(*) FROM %s GROUP BY uid ORDER BY count DESC;", snapshot);
  else if (query_type == USAGE)
    snprintf(query, 80, "SELECT uid,sum(block*block_size) FROM %s GROUP BY uid ORDER BY sum DESC;", snapshot);
  else if (query_type == GROUP)
    snprintf(query, 80, "SELECT gid,sum(size) FROM %s GROUP BY gid ORDER BY sum DESC;", snapshot);

  query_res = PQexec(conn, query);
  if (PQresultStatus(query_res) != PGRES_TUPLES_OK) {
    fprintf(stderr, "SELECT uid,sum(size) command failed: %s", PQerrorMessage(conn));
    PQclear(query_res);
    PQfinish(conn);
    return -1;
  }

  if ((count > 0) && (count < PQntuples(query_res)))
    results = count;
  else
    results = PQntuples(query_res);

  if (query_type == SIZE) {
    printf("    Top %i users are:\n", results);
    printf("      User   Percent   Gbytes\n");
    printf("      ----   -------   ------\n");
    
    
    for (i = 0; i < results; i++) {
      pw = getpwuid((uid_t)atoi(PQgetvalue(query_res, i, 0)));
      if (pw)
	printf("%10s %9.2f %8lu\n", pw->pw_name, ((atol(PQgetvalue(query_res, i, 1))/(1024.0 * 1024.0 * 1024.0))/(double)gb) * 100.0, (atol(PQgetvalue(query_res, i, 1))/(1024 * 1024 * 1024)));
      else
	printf("%10s %9.2f %8lu\n", PQgetvalue(query_res, i, 0), ((atol(PQgetvalue(query_res, i, 1))/(1024.0 * 1024.0 * 1024.0))/(double)gb) * 100.0, (atol(PQgetvalue(query_res, i, 1))/(1024 * 1024 * 1024)));
    }
  }
  else if (query_type == USAGE) {
    printf("    Top %i users are:\n", results);
    printf("      User   Percent   Gbytes\n");
    printf("      ----   -------   ------\n");
    
    
    for (i = 0; i < results; i++) {
      pw = getpwuid((uid_t)atoi(PQgetvalue(query_res, i, 0)));
      if (pw)
	printf("%10s %9.2f %8lu\n", pw->pw_name, ((atol(PQgetvalue(query_res, i, 1))/(1024.0 * 1024.0 * 1024.0 * 10))/(double)gb) * 100.0, (atol(PQgetvalue(query_res, i, 1))/(1024 * 1024 * 1024))/10);
      else
	printf("%10s %9.2f %8lu\n", PQgetvalue(query_res, i, 0), ((atol(PQgetvalue(query_res, i, 1))/(1024.0 * 1024.0 * 1024.0 * 10))/(double)gb) * 100.0, (atol(PQgetvalue(query_res, i, 1))/(1024 * 1024 * 1024))/10);
    }
  }
  else if (query_type == NFILES) {
    printf("    Top %i users are:\n", results);
    printf("      User   #Files(including directories)\n");
    printf("      ----   ------\n");
    
    
    for (i = 0; i < results; i++) {
      pw = getpwuid((uid_t)atoi(PQgetvalue(query_res, i, 0)));
      if (pw)
	printf("%10s %8s\n", pw->pw_name, PQgetvalue(query_res, i, 1));
      else
	printf("%10s %8s\n", PQgetvalue(query_res, i, 0), PQgetvalue(query_res, i, 1));
    }
  }
  else if (query_type == GROUP) {
    printf("    Top %i groups are:\n", results);
    printf("      Group  Percent   Gbytes\n");
    printf("      -----  -------   ------\n");
    
    
    for (i = 0; i < results; i++) {
      gr = getgrgid((gid_t)atoi(PQgetvalue(query_res, i, 0)));
      if (gr)
	printf("%10s %9.2f %8lu\n", gr->gr_name, ((atol(PQgetvalue(query_res, i, 1))/(1024.0 * 1024.0 * 1024.0))/(double)gb) * 100.0, (atol(PQgetvalue(query_res, i, 1))/(1024 * 1024 * 1024)));
      else
	printf("%10s %9.2f %8lu\n", PQgetvalue(query_res, i, 0), ((atol(PQgetvalue(query_res, i, 1))/(1024.0 * 1024.0 * 1024.0))/(double)gb) * 100.0, (atol(PQgetvalue(query_res, i, 1))/(1024 * 1024 * 1024)));
    }
  }
  
  PQclear(query_res);
  
  PQfinish(conn);
  
  return 0;
}

void usage() {
  printf("usage: checkscratch [options] [number]\n");
  printf("       -s    sum of file and directory size (default)\n");
  printf("       -n    number of files and directories size\n");
  printf("       -u    sum of file and directory usage (on disk)\n");
  printf("       -g    sum of file and directory size by group\n");
  printf("       -h    usage\n");
  printf("\n");
  printf("       number specifies how many entries should be printed\n");
  printf("          default is to print all\n");
}

int main(int argc, char *argv[]) {
  int results, ret, gb, c;
  int option_index = 0;
  int query_type = SIZE;
  dbinfo_t dbinfo;

  while ((c = getopt_long(argc, argv, "snugh", NULL, &option_index)) != -1) {
    switch (c)
      {
      case 's':
	query_type = SIZE;
        break;
      case 'n':
	query_type = NFILES;
        break;
      case 'u':
	query_type = USAGE;
        break;
      case 'g':
	query_type = GROUP;
        break;
      case 'h':
        usage(0);
        return 0;
        break;
      default:
        return 0;
      }
  }

  if (argc > optind)
    results = atoi(argv[optind]);
  else
    results = 0;

  /*put in config parser*/

  gb = printstat("scratch1");
  printf("\n");
  ret = printusage("scratch1", results, gb, query_type, dbinfo);

  printf("\n");
  printf("\n");
  gb = printstat("scratch2");
  printf("\n");
  ret = printusage("scratch2", results, gb, query_type, dbinfo);

  //printf("\n");
  //printf("\n");
  //gb = printstat("scratch1");
  //ret = printusage("scratch1", results, gb, query_type, dbinfo);

  printf("\n");
  return ret;
}

