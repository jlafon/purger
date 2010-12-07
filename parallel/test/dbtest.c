#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libpq-fe.h>

#define CFG_FILE "db.conf"

struct dbinfo_t {
  char host[256];
  char port[16];
  char user[256];
  char pass[256];
};
typedef struct dbinfo_t dbinfo_t;

int main() {
  PGconn *conn;
  PGresult *snapshot;
  dbinfo_t dbinfo;

  /*put in config parser*/
 
  conn = PQsetdbLogin(dbinfo.host, dbinfo.port, NULL, NULL, "scratch1", dbinfo.user, dbinfo.pass);
  
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
