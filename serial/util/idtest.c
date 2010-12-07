#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>

int main() {
  PGconn *conn;
  PGresult *res;
  int id;
  char query[500];

  conn = PQsetdbLogin("db.localdomain", "5432", NULL, NULL, "scratch2", "treewalk", "testing");
  if (PQstatus(conn) != CONNECTION_OK) {
    fprintf(stderr, "Connection to database failed: %s",
	    PQerrorMessage(conn));
    PQfinish(conn);
    return 1;
  }
  
  printf("Successful connection to DB!\n");
  
  /*
  res = PQexec(conn, "SELECT last_value FROM performance_id_seq;");
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    fprintf(stderr, "SELECT command failed: %s\n", PQerrorMessage(conn));
    PQclear(res);
    return 1;
  }
  
  printf("Current id is: %s\n", PQgetvalue(res, 0, 0));
  id = atoi(PQgetvalue(res, 0, 0));
  id++;
  */

  res = PQexec(conn, "INSERT INTO performance (error) VALUES (1) RETURNING id;");
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    fprintf(stderr, "INSERT command failed: %s\n", PQerrorMessage(conn));
    PQclear(res);
    return 1;
  }

  printf("Current id is: %s\n", PQgetvalue(res, 0, 0));
  id = atoi(PQgetvalue(res, 0, 0));

  snprintf(query, 500,"UPDATE performance SET files=1, directories=2, runtime=3, ranks=4, packsize=5, error=0 WHERE id = %i;", id); 
  res = PQexec(conn, query);
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    fprintf(stderr, "UPDATE command failed: %s\n", PQerrorMessage(conn));
    PQclear(res);
    return 1;
  }

  PQclear(res);
  
  PQfinish(conn);
  return 0;
}

