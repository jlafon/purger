#include <stdio.h>
#include <string.h>
#include <libpq-fe.h>
#include <getopt.h>

#define CFG_FILE "db.conf"
#define PATHSIZE_PLUS 1024

struct dbinfo_t {
  char host[256];
  char port[16];
  char user[256];
  char pass[256];
};
typedef struct dbinfo_t dbinfo_t;

int update(const char* filesystem, dbinfo_t dbinfo) {
  PGconn *conn;
  PGresult *snapshot_res;
  PGresult *query_res;
  char snapshot[30];
  char query[PATHSIZE_PLUS];

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
  
  snprintf(snapshot, 30, (PQgetvalue(snapshot_res, 0, 0)));

  PQclear(snapshot_res);
  
  snprintf(query, 500, "SELECT merge(filename, uid, atime, mtime, ctime) FROM %s WHERE (atime <= (now() - '14 days'::interval)) AND (mtime <= (now() - '14 days'::interval)) AND (ctime <= (now() - '14 days'::interval));", snapshot);
  query_res = PQexec(conn, query);
  if (PQresultStatus(query_res) != PGRES_TUPLES_OK) {
    fprintf(stderr, "SELECT command failed: %s", PQerrorMessage(conn));
    PQclear(query_res);
    PQfinish(conn);
    return -1;
  }
  
  PQclear(query_res);
  PQfinish(conn);
  
  return 0;
}

void usage() {
  printf("usage: update_expired <database>\n");
  printf("       -h    usage\n");
  printf("\n");
}

int main(int argc, char *argv[]) {
  int ret, c;
  int option_index = 0;
  dbinfo_t dbinfo;
  char dbname[40];

  while ((c = getopt_long(argc, argv, "h", NULL, &option_index)) != -1) {
    switch (c)
      {
      case 'h':
        usage(0);
        return 0;
        break;
      default:
        return 0;
      }
  }

  if (argc > optind)
    snprintf(dbname, 40, "%s",argv[optind]);
  else {
    fprintf(stderr, "Must specify database\n");
    return -1;
  }

  /*put in config parser */

  printf("warning: this could take a little while...\n");
  fflush(stdout);

  ret = update(dbname, dbinfo);

  printf("%s updated.\n", dbname);
  return ret;
}

