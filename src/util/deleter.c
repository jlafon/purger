/** 
  * \file deleter.c
  * \author Ben McClelland 
  * \date 06/07/2011
  */
#include "deleter.h"
#include "lconfig.h"

//! Main function

int main(int argc, char *argv[]){

  PGconn    *conn;
  PGresult  *files;
  char 	    query[4096];
  char      filesystem[1024];
  dbinfo_t  dbinfo;

  if (argc!=2){
    fprintf(stderr, "usage: purger <filesystem>\n");
    return 1;
  }
  else if (strncpy(filesystem, argv[1], 1024)==NULL){
    fprintf(stderr, "error initializing filesystem\n");
    return 1;
  }

  if (parse_config_dbonly(&dbinfo) != EXIT_SUCCESS)
  {
    fprintf(stderr, "Unable to parse config file.\n");
    exit(-1);
  }
  
  conn = PQsetdbLogin(dbinfo.host, dbinfo.port, NULL, NULL, filesystem, dbinfo.user, dbinfo.pass);
  
  if (PQstatus(conn) != CONNECTION_OK)
    {
      fprintf(stderr, "Connection to database failed: %s",
	      PQerrorMessage(conn));
      exit_nicely(conn);
    }
  
  sprintf(query, "SELECT filename FROM snapshot1 WHERE filename like '/panfs/scratch1/vol%%/%%/_%%' AND atime <= now() - INTERVAL '1 month' AND ctime <= now() - INTERVAL '1 month' AND mtime <= now() - INTERVAL '1 month';"); 
  files = PQexec(conn, query);
  if (PQresultStatus(files) != PGRES_TUPLES_OK)
    {
      fprintf(stderr, "SELECT * command failed: %s", PQerrorMessage(conn));
      PQclear(files);
      exit_nicely(conn);
    }
  
  process_files(conn, files);

  PQclear(files);
  
  
  /* close the connection to the database and cleanup */
  PQfinish(conn);
  
  return 0;
}
//! Exits successfully
static void exit_nicely(PGconn *conn){
  PQfinish(conn);
  exit(1);
}
//! Function the processes files
int process_files(PGconn *conn, PGresult *files){
  //counting variables
  int i;
  FILE *file;

  file = fopen("/tmp/purge.log","a+");
  
  // ------      BEGIN PROCESSING      ------
  // For each file:
  for (i = 0; i < PQntuples(files); i++){
    fprintf(file,"%s\n", PQgetvalue(files, i, 0));
    unlink(PQgetvalue(files, i, 0));
  }

  fclose(file);
  
  return 0;
}
