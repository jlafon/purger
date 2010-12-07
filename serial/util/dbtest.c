#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>

int main() {
  PGconn *conn;
  PGresult *snapshot;

  conn = PQsetdbLogin("db.localdomain", "5432", NULL, NULL, "scratch2", "treewalk", "testing");
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

