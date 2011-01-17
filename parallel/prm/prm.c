#include "prm.h"

void catch_alarm (int sig) {
  struct timeval tv;
  time_t now;
  int timenow=0;
  
  gettimeofday(&tv, NULL);
  now = tv.tv_sec;
  timenow = (int)now;
  
  printf("\33[2J\n");
  printf("     DELETED     REJECTED          SUM        TOTAL      RUNTIME\n");   
  printf("\r%12i %12i %12i %12i %12i",delcnt, rejcnt, sumcnt, totcnt, (timenow - starttime));
  fflush(stdout);
  
  alarm(WAIT_TIME);
}

/***********************************************************************************************
 * Function: main 
 *
 * Description:  Main interprets input arguments, initializes variables and MPI, and assigns 
 * roles to processes. 
 *
 ***********************************************************************************************/
int main( int argc, char *argv[] )
{
  int rank;
  int nproc;
  struct timeval tv;
  int db_defined=0;
  char db_name[PATHSIZE_PLUS];
  dbinfo_t dbinfo;
  static struct option long_options[] =
  {
       {"db",          required_argument, 0, 'd'},
       {"help",        no_argument,       0, 'h'},
       {0,0,0,0}
  };
  
  int option_index = 0;
  int c;
  
  gettimeofday(&tv, NULL);
  starttime = (int)tv.tv_sec;
  memset(db_name, '\0', sizeof(char) * PATHSIZE_PLUS);
  while ((c = getopt_long(argc, argv, "d:h", long_options, &option_index)) != -1) {
    switch (c) {
    case 'd':
      db_defined = 1;
      snprintf(db_name, PATHSIZE_PLUS, "%s", optarg); 
      break;
    case 'h':
      /*help? why would I help you*/
      usage(0);
      return(0);
      break;
      
    default:
      return(0);
    }
  }
  
  if (db_defined != 1) {
    fprintf(stderr, "ERROR: need to specify db\n");
    return -1;
  }
  
  if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
    fprintf(stderr, "ERROR: Unable to initialize MPI.\n");
    return -1;
  }
  
  if (MPI_Comm_size(MPI_COMM_WORLD, &nproc) != MPI_SUCCESS) {
    fprintf(stderr, "ERROR: Unable to acquire number of processes in MPI_COMM_WORLD.\n");
    return -1;
  }
  
  if (MPI_Comm_rank(MPI_COMM_WORLD, &rank) != MPI_SUCCESS) {
    fprintf(stderr, "ERROR: Unable to acquire rank.\n");
    return -1;
  }
  
  /*We need at least 2 procs... 1 manager and 1+ workers*/
  if (nproc < 2) {
    fprintf(stderr, "\nERROR: %d processes specified, must specify at least 2 processes.\n", nproc);
    MPI_Finalize();
    return -1;
  }
  
  if (strlen(db_name) < 1) {
    fprintf(stderr, "Need to specify DATABASE name\n");
    MPI_Finalize();
    return -1;
  }
  
  /*add parse_config here*/
  
  if(parse_config_dbonly(&dbinfo)< 0)
  {
    fprintf(stderr, "Configurationf file error.\n");
    MPI_Finalize();
    return -1;
  }
  
  conn = PQsetdbLogin(dbinfo.host, dbinfo.port, NULL, NULL, db_name, dbinfo.user, dbinfo.pass);
  
  if (PQstatus(conn) != CONNECTION_OK) {
    fprintf(stderr, "Connection to database %s failed: %s", db_name, 
	    PQerrorMessage(conn));
    PQfinish(conn);
    MPI_Finalize();
    return -1;
  }
  
  /*assign the different ranks the jobs they will do 0-manager, rest workers*/
  if (rank == 0) {
    signal (SIGALRM, catch_alarm);
    alarm(WAIT_TIME);
    manager(db_name, nproc);
  }
  else
    worker(rank);
  
  PQfinish(conn);
  MPI_Finalize();
  
  return 0;
}


/***********************************************************************************************
 * Function: manager (rank 0)  
 ***********************************************************************************************/
void manager(char *db_name, int nproc)
{
  
  MPI_Status status;
  int position = 0;
  int type_cmd;
  int rejected = 0; 
  int deleted = 0;

  char* workbuf = (char*) malloc(WORKSIZE * sizeof(char));
  char abortcmd[10];
  
  int all_done = 0;
  int i, j;
  int cursor = 0;
  int inproc;
  int received_cmd;

  long tempa, tempm, tempc;

  char query[1024];
  PGresult *files;
  PGresult *uids;
  PGresult *res;

  char* path = (char*) malloc(PATHSIZE_PLUS * sizeof(char));

  sprintf(abortcmd,"%d",ABORTCMD);
  
  /* malloc space, set up proc_status one slot for every nproc */
  if ((proc_status = (int *)malloc(nproc*(sizeof(int)))) == (int *)0) {
    fprintf(stderr, "Error allocating memory: %s\n\n", "proc_status");
    send_abort_cmd(abortcmd, nproc);
  }
  
  /* initialize all slots in proc_status to not busy */
  for (i = 0; i < nproc; i++) 
    proc_status[i]=0;

  uids = PQexec(conn, "SELECT DISTINCT(uid) FROM exceptions WHERE expiration > now();");
  if (PQresultStatus(uids) != PGRES_TUPLES_OK) {
    fprintf(stderr, "SELECT DISTINCT(uid) command failed: %s", 
                PQerrorMessage(conn));
    PQclear(uids);
    fprintf(sterr, "bailing to protect execptions.\n");
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  
  for (i = 0; i < PQntuples(uids); i++) {
    snprintf(query, 1024, "UPDATE expired_files SET warned = False WHERE uid=%s;", PQgetvalue(uids, i, 0));
    res = PQexec(conn, update_query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      fprintf(stderr, "update warned command failed: %s", PQerrorMessage(conn));
      PQclear(res);
      fprintf(sterr, "bailing to protect execptions.\n");
      MPI_Abort(MPI_COMM_WORLD, -1);
    } 
  }
  
  PQclear(uids);

  snprintf(query, 1024, "SELECT filename, extract(epoch from atime at time zone 'GMT') as atime, extract(epoch from mtime at time zone 'GMT') as mtime, extract(epoch from ctime at time zone 'GMT') as ctime FROM expired_files where filename like '/panfs/%s/vol%%/%%/_%%' AND filename NOT like '/panfs/%s/vol%%/.plfs_store' AND uid != 0 AND warned = True AND added < CURRENT_TIMESTAMP - INTERVAL '6 days';", db_name);
  files = PQexec(conn, query);
  if (PQresultStatus(files) != PGRES_TUPLES_OK) {
    fprintf(stderr, "SELECT * command failed: %s", PQerrorMessage(conn));
    PQclear(files);
    PQfinish(conn);
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  
  printf("\n0: got %i files to process\n", PQntuples(files));
  fflush(stdout);
  totcnt = PQntuples(files);

  /* loop until we get through all files */
  while (all_done == 0) {
    /* recv message from anyone */
    if (MPI_Recv(workbuf, WORKSIZE, MPI_PACKED, MPI_ANY_SOURCE, MPI_ANY_TAG, 
		 MPI_COMM_WORLD,&status) != MPI_SUCCESS) {
      fprintf(stderr, "ERROR in manager when receiving message.\n");  
      MPI_Abort(MPI_COMM_WORLD, -1);
    }
    
    position = 0;
    MPI_Unpack(workbuf, WORKSIZE, &position, &received_cmd, 1, MPI_INT, MPI_COMM_WORLD);
    
    switch (received_cmd) {
    case RESCMD:
      MPI_Unpack(workbuf, WORKSIZE, &position, &deleted, 1, MPI_INT, MPI_COMM_WORLD);
      MPI_Unpack(workbuf, WORKSIZE, &position, &rejected, 1, MPI_INT, MPI_COMM_WORLD);
      
      delcnt += deleted;;
      rejcnt += rejected;
      sumcnt += deleted;
      sumcnt += rejected;
      break;
    case REQCMD:
      MPI_Unpack(workbuf, WORKSIZE, &position, &inproc, 1, MPI_INT, MPI_COMM_WORLD);
      /* mark this worker free in the proc_status */
      proc_status[inproc] = 0;
      
      j=0;
      for (i = 0; i < nproc; i++) 
	j += proc_status[i];

      if (cursor < PQntuples(files)) {
	position = 0;
	type_cmd = DELCMD;
	if (MPI_Send (&type_cmd, 1, MPI_INT, inproc, inproc, MPI_COMM_WORLD ) != MPI_SUCCESS) {
	  fprintf(stderr, "ERROR when manager (rank 0) attempted to send DELCMD.\n");  
	  MPI_Abort(MPI_COMM_WORLD, -1);
	}
	
	for (i = 0; (((i+cursor) < PQntuples(files)) && (i < PACKSIZE)); i++, cursor++){
	  sprintf(path,"%s",PQgetvalue(files,cursor,0));
	  tempa=atol(PQgetvalue(files,cursor,1));
	  tempm=atol(PQgetvalue(files,cursor,2));
	  tempc=atol(PQgetvalue(files,cursor,3));

	  MPI_Pack(path, PATHSIZE_PLUS, MPI_CHAR, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
	  MPI_Pack(&tempa, 1, MPI_LONG, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
	  MPI_Pack(&tempm, 1, MPI_LONG, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
	  MPI_Pack(&tempc, 1, MPI_LONG, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
	}
	
	if (MPI_Send (&i, 1, MPI_INT, inproc, inproc, MPI_COMM_WORLD ) != MPI_SUCCESS) {
	  fprintf(stderr, "ERROR when manager (rank 0) attempted to send count.\n");  
	  MPI_Abort(MPI_COMM_WORLD, -1);
	}

	if (MPI_Send (workbuf, position, MPI_PACKED, inproc, inproc, MPI_COMM_WORLD ) != MPI_SUCCESS) {
	  fprintf(stderr, "ERROR when manager (rank 0) attempted to send list.\n");  
	  MPI_Abort(MPI_COMM_WORLD, -1);
	}
      }
      else if (j == 0) {
	for (i = 1; i < nproc; i++) {
	  type_cmd = EXITCMD;
	  position = 0;
	  MPI_Pack(&type_cmd, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
	  MPI_Pack(&i, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
	  
	  if (MPI_Send (workbuf, position, MPI_PACKED, i, i, MPI_COMM_WORLD ) != MPI_SUCCESS) {
	    fprintf(stderr, "ERROR in manager sending when sending exit to slaves.\n");  
	    MPI_Abort(MPI_COMM_WORLD, -1);
	  }
	}
	all_done = 1;
      }
      break;
    } /*switch received command*/
  } /*while not all done*/
  
  /*cleanup memory*/
  free(proc_status);
  free(workbuf);
}

/***********************************************************************************************
 * Function: worker  (rank 1 and above)  (workers)
 *
 * Description:  This function receives a MPI message from the manager that contains either a  
 *               directory or a list of filenames. If new directories are found, these are 
 *               reported back to the manager.
 *
 ***********************************************************************************************/
void worker(int rank)
{
  int all_done=0;
  char logname[100];
  MPI_Status status;
  struct stat st;
  int position=0;
  int type_cmd;
  int abort=0;
  int count=0;
  int deleted, rejected;
  int i;
  stinfo_t stats;

  FILE *log;

  PGresult *queryres;
  char query[500];

  time_t rawtime;

  /*initialize MPI send/recv buffer*/
  char* workbufin = (char*) malloc(WORKSIZE * sizeof(char));
  char* workbufout = (char*) malloc(WORKSIZE * sizeof(char));

  char path[PATHSIZE_PLUS];

  snprintf(logname, 100, "/scratch2/ben/PURGER/logs/%i", rank);
  log = fopen(logname, "a");
  if (!log) {
    perror("unable to open log");
    MPI_Abort(MPI_COMM_WORLD, -1);
  }

  time(&rawtime);
  fprintf(log, "\n%s\n", ctime(&rawtime));
  
  type_cmd = REQCMD;
  position = 0;
  MPI_Pack(&type_cmd, 1, MPI_INT, workbufout, WORKSIZE, &position, MPI_COMM_WORLD);
  MPI_Pack(&rank, 1, MPI_INT, workbufout, WORKSIZE, &position, MPI_COMM_WORLD);
  
  if (MPI_Send(workbufout, position, MPI_PACKED, MANAGER_PROC, MANAGER_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) {
    fprintf(stderr, "ERROR in worker sending message.\n");  
    MPI_Abort(MPI_COMM_WORLD, -1);
  }
  
  while (all_done == 0) {
    /*get our next task*/
    if (MPI_Recv(&type_cmd, 1, MPI_INT, MANAGER_PROC, rank, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
      fprintf(stderr, "ERROR in worker receiving.\n");  
      MPI_Abort(MPI_COMM_WORLD, -1);
    }
    
    switch(type_cmd) {
      /*if we get an ABORT, lets do so*/
    case ABORTCMD:
      abort=1;
      break;
      
    case DELCMD:
      deleted = rejected = 0;
      
      if (MPI_Recv(&count, 1, MPI_INT, MANAGER_PROC, rank, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
	fprintf(stderr, "ERROR in worker receiving.\n");  
	MPI_Abort(MPI_COMM_WORLD, -1);
      }
      
      if (MPI_Recv(workbufin, WORKSIZE, MPI_PACKED, MANAGER_PROC, rank, MPI_COMM_WORLD, &status) != MPI_SUCCESS) {
	fprintf(stderr, "ERROR in worker receiving.\n");  
	MPI_Abort(MPI_COMM_WORLD, -1);
      }
      
      position = 0;
      for (i=0; i<count; i++) {
	MPI_Unpack(workbufin, WORKSIZE, &position, &path, PATHSIZE_PLUS, MPI_CHAR, MPI_COMM_WORLD);
	MPI_Unpack(workbufin, WORKSIZE, &position, &stats.atime, 1, MPI_LONG, MPI_COMM_WORLD);
	MPI_Unpack(workbufin, WORKSIZE, &position, &stats.mtime, 1, MPI_LONG, MPI_COMM_WORLD);
	MPI_Unpack(workbufin, WORKSIZE, &position, &stats.ctime, 1, MPI_LONG, MPI_COMM_WORLD);
	
	if (lstat(path,&st) == -1) {
          fprintf(stderr,"ERROR: %s\n",path);
          perror("worker(), lstat()");
        }
	
	/* exception check */
	
	if ((st.st_atime == stats.atime) && (st.st_mtime == stats.mtime) && (st.st_ctime == stats.ctime)) {
	  /* delete file */
	  if (remove(path) == 0) {
	    fprintf(log, "%s %ju %ju %ju %i\n", path, st.st_atime, st.st_mtime, st.st_ctime, st.st_uid);
	    deleted++;
	  }
	  else
	    rejected++;
	}
	else
	  rejected++;
	
	snprintf(query, 500, "DELETE FROM expired_files where filename='%s';", path);
	queryres = PQexec(conn, query);
	if (PQresultStatus(queryres) != PGRES_COMMAND_OK) {
	  fprintf(stderr, "DELETE * command failed: %s", PQerrorMessage(conn));
	  PQclear(queryres);
	  PQfinish(conn);
	  MPI_Abort(MPI_COMM_WORLD, -1);
	}
      }
      
      type_cmd = RESCMD; 
      position = 0;
      MPI_Pack(&type_cmd, 1, MPI_INT, workbufout, WORKSIZE, &position, MPI_COMM_WORLD);
      MPI_Pack(&deleted, 1, MPI_INT, workbufout, WORKSIZE, &position, MPI_COMM_WORLD);
      MPI_Pack(&rejected, 1, MPI_INT, workbufout, WORKSIZE, &position, MPI_COMM_WORLD);

      if (MPI_Send(workbufout, position, MPI_PACKED, MANAGER_PROC, MANAGER_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) {
	fprintf(stderr, "ERROR in worker sending message.\n");  
	MPI_Abort(MPI_COMM_WORLD, -1);
      }
      
      /* done with the list, send workreq to manager to get more work or finish up */
      type_cmd = REQCMD;
      position = 0;
      MPI_Pack(&type_cmd, 1, MPI_INT, workbufout, WORKSIZE, &position, MPI_COMM_WORLD);
      MPI_Pack(&rank, 1, MPI_INT, workbufout, WORKSIZE, &position, MPI_COMM_WORLD);
      
      if (MPI_Send(workbufout, position, MPI_PACKED, MANAGER_PROC, MANAGER_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) {
	fprintf(stderr, "ERROR in worker sending message.\n");  
	MPI_Abort(MPI_COMM_WORLD, -1);
      }
      break;
      
    case EXITCMD:
      all_done=1;
      break;
    }
    
    if (abort == 1) break;
  }

  if (fclose(log) != 0)
    perror("closing log");
  
  /*cleanup memory allocation*/  
  free(workbufin);
  free(workbufout);
}
