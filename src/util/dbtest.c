#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include "config.h"
struct dbinfo_t {
  char host[256];
  char port[16];
  char user[256];
  char pass[256];
};
typedef struct dbinfo_t dbinfo_t;


int main(int argc, char *argv[]){
  PGconn *conn;
  PGresult *snapshot;
  dbinfo_t dbinfo;

  if (parse_config_dbonly(&dbinfo) != 0){
    fprintf(stderr, "error parsing config\n");
    return 1;
  }
  
  if (argc<2){
    fprintf(stderr, "need to specify database\n");
    return 1;
  }

  fprintf(stdout, "host: %s\nport: %s\nuser: %s\npass: %s\ndatabase: %s\n", dbinfo.host, dbinfo.port, dbinfo.user, dbinfo.pass, argv[1]);

  conn = PQsetdbLogin(dbinfo.host, dbinfo.port, NULL, NULL, argv[1], dbinfo.user, dbinfo.pass);
  if (PQstatus(conn) != CONNECTION_OK) {
    fprintf(stderr, "Connection to database failed: %s",
	    PQerrorMessage(conn));
    PQfinish(conn);
    return 1;
  }
  
  printf("Successful connection to DB!\n");
  
  snapshot = PQexec(conn, "SELECT name FROM current_snapshot WHERE ID = 1;");
  if (PQresultStatus(snapshot) != PGRES_TUPLES_OK) {
    fprintf(stderr, "SELECT current_snapshot command failed: %s\n", PQerrorMessage(conn));
    PQclear(snapshot);
    return 1;
  }
  
  printf("Current snapshot is: %s\n", PQgetvalue(snapshot, 0, 0));
  PQclear(snapshot);
  
  PQfinish(conn);
  return 0;
}

