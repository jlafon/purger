/************************************************************************************
* \file pstat.c
* Name:  purger
*
* Description:  
* 
* \authors Gary Grider,Alfred Torrez,Ben McClelland,Jharrod LaFon
*
* History:
* 
* 11/30/2006 General cleanup of code 
* 04/02/2007 Fixed directory name bugs (% symbol and spaces in directory name)
*            as well as cleanup (moved utility functions to ftw_utils.c).
* 08/20/2007 adapted for use as scratch filesystem purger (code re-write)
*
*************************************************************************************/

#include "pstat.h"

int dreqcnt=0;
int nreqcnt=0;
int starttime=0;
int shutdown=0;
char lastpath[PATHSIZE_PLUS];
int *proc_status;

PGconn *conn;
char snapshot_name[16];

//! Alarm handler
void catch_alarm (int sig) {
  struct timeval tv;
  time_t now;
  int timenow=0;
  
  gettimeofday(&tv, NULL);
  now = tv.tv_sec;
  timenow = (int)now;
  
  printf("\33[2J\n");
  printf("NAME_QUEUE_SIZE    DIR_QUEUE_SIZE          RUNTIME\n");   
  printf("\r%8i %15i %22i seconds  %s",nreqcnt, dreqcnt, (timenow - starttime), lastpath);
  fflush(stdout);
  
  alarm(WAIT_TIME);
}

//! Signal Handler
void sig_cleanup (int sig) {
  shutdown=1;
  printf("\n\nCaught signal %i.  Writing restart and shutting down...\n", sig);
  fflush(stdout);
}

//! Signal Handler for USER1
void sig_user1 (int sig) {
  int nproc, i;
  if (MPI_Comm_size(MPI_COMM_WORLD, &nproc) != MPI_SUCCESS) {
    fprintf(stderr, "ERROR: Unable to acquire number of processes in MPI_COMM_WORLD.\n");
    return;
  }

  printf("\n");
  for (i=0; i<nproc; i++) 
    printf("%i[%i] ", i, proc_status[i]);

  fflush(stdout);
}

//! Parses out the parent of a path
int split_path(char * parent,char * path, int max_length)
{
    int length = strnlen(path);
    char x = path[length-1];
    path[length-1] = 'X';
    char * p = strrchr(path,'/');
    path[length-1] = x;
    strncpy(parent,path,p-path+1);
    parent[p-path+1] = '\0';
    return p-path+2;
}

//! Converts decimal numbers to binary
void dec2bin(long decimal, char *binary)
{
  int  k = 0, n = 0, count = 0;
  int  neg_flag = 0;
  int  remain;
  int  old_decimal;  // for test
  char temp[80];

  // take care of negative input
  if (decimal < 0)
  {      
    decimal = -decimal;
    neg_flag = 1;
  }
  
  for(count=0; count < 32; count++)
  {
    old_decimal = decimal;   // for test
    remain    = decimal % 2;
    // whittle down the decimal number
    decimal   = decimal / 2;
    // converts digit 0 or 1 to character '0' or '1'
    temp[k++] = remain + '0';
  }

  // reverse the spelling
  while (k >= 0)
    binary[n++] = temp[--k];

  binary[n-1] = 0;         // end with NULL
}


/***********************************************************************************************
 * Function: main 
 *
 * Description:  Main interprets input arguments, initializes variables and MPI, and assigns 
 * roles to processes. 
 *
 ***********************************************************************************************/
//! Main
int main( int argc, char *argv[] )
{
  int rank;
  int nproc;
  struct timeval tv;
  time_t now;
  int timenow=0;

  PGresult *snapshot;
  PGresult *res;
  char db_name[PATHSIZE_PLUS], pgcmd[4096];
  char restart_name[PATHSIZE_PLUS];

  static struct option long_options[] =
  {
       {"db",          required_argument, 0, 'd'},
       {"path",        required_argument, 0, 'p'},
       {"restart",     required_argument, 0, 'r'},
       {"help",        no_argument,       0, 'h'},
       {0,0,0,0}
  };
 
  char beginning_path[PATHSIZE_PLUS];
  int option_index = 0;
  int c;
  int path_specified = 0;
  int id;

  dbinfo_t dbinfo;

  gettimeofday(&tv, NULL);
  now = tv.tv_sec;
  timenow = (int)now;
  starttime = timenow;
  
  while ((c = getopt_long(argc, argv, "d:p:r:h", long_options, &option_index)) != -1) {
    switch (c)
      {
      case 'p':
	/*this is the root of our directory tree*/
        snprintf(beginning_path, PATHSIZE_PLUS, "%s", optarg); 
        path_specified = 1;
        break;
      case 'd':
	/*turn on database*/
	db_on = 1;
	snprintf(db_name, PATHSIZE_PLUS, "%s", optarg); 
	break;
      case 'r':
	/*start from restart files*/
	restart = 1;
	snprintf(restart_name, PATHSIZE_PLUS, "%s", optarg);
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

  /*we really do need a starting point, not sure what default would make sense
    "/" seems scary*/
  if (path_specified != 1 ) {
    if (rank == 0)
      fprintf(stderr, "\nERROR:  Path must be specified with --path path\n\n");
    MPI_Finalize();
    return -1; 
  }

  /*We need at least 3 procs... 1 manager, 1 readdir, and 1+ workers*/
  if (nproc < 3) {
    fprintf(stderr, "\nERROR: %d processes specified, must specify at least 3 processes.\n", nproc);
    MPI_Finalize();
    return -1;
  }

  if (db_on) {
    if (strlen(db_name) < 1) {
      fprintf(stderr, "Need to specify DATABASE name\n");
      MPI_Finalize();
      return -1;
    }

    /*add parse_config here*/
    if(parse_config_dbonly(&dbinfo) != EXIT_SUCCESS)
    { 
	fprintf(stderr,"Error parsing config file, exiting.\n");
	fprintf(stderr,"Values parsed are: \n\
		\thost: %s\n\
		\tport: %d\n\
		\tname: %s\n\
		\tuser: %s\n",dbinfo.host, dbinfo.port,db_name,dbinfo.user);
	exit(-1);
    }
    
    conn = PQsetdbLogin(dbinfo.host, dbinfo.port, NULL, NULL, db_name, dbinfo.user, dbinfo.pass);
	
    if (PQstatus(conn) != CONNECTION_OK) {
      fprintf(stderr, "Connection to database %s failed: %s", db_name, 
	      PQerrorMessage(conn));
      PQfinish(conn);
      MPI_Finalize();
      return -1;
    }
    
    if (rank == 0) {
      if (db_on) {
	res = PQexec(conn, "INSERT INTO performance (error) VALUES (1) RETURNING id;");
	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
	  fprintf(stderr, "[%s][%d] INSERT command failed: %s\n", __FILE__,__LINE__,PQerrorMessage(conn));
	  PQclear(res);
	  return 1;

      }
	id = atoi(PQgetvalue(res, 0, 0));
      }
    }

    snapshot = PQexec(conn, "SELECT name FROM current_snapshot WHERE ID = 1;");
    
    if (PQresultStatus(snapshot) != PGRES_TUPLES_OK) {
      fprintf(stderr, "SELECT current_snapshot command failed: %s\n", PQerrorMessage(conn));
      PQclear(snapshot);
      MPI_Finalize();
      return -1;
    }
    
    if (strncmp(PQgetvalue(snapshot, 0, 0), "snapshot1", 16) == 0)
      snprintf(snapshot_name, 16, "%s", "snapshot2");
    else if (strncmp(PQgetvalue(snapshot, 0, 0), "snapshot2", 16) == 0)
      snprintf(snapshot_name, 16, "%s", "snapshot1");

    if (!((strncmp(snapshot_name, "snapshot1", 16) == 0) || (strncmp(snapshot_name, "snapshot2", 16) == 0))) {
      fprintf(stderr, "could not get snapshot table from current_snapshot (got: %s)\n", PQgetvalue(snapshot, 0, 0));
      MPI_Finalize();
      return -1;
    }

    if (db_on) {
      PQclear(snapshot);
      if (rank == 0) PQclear(res);
    }
  }
  
  if (restart) {
    if (strlen(restart_name) < 1) {
      fprintf(stderr, "Need to specify restart name to restart from file\n");
      MPI_Finalize();
      return -1;
    }
  }

   /*assign the different ranks the jobs they will do 0-manager, rest workers*/
  if (rank == 0) {
    if (db_on) {
      snprintf(pgcmd, 4096, "TRUNCATE TABLE %s;", snapshot_name);
      snapshot = PQexec(conn, pgcmd);
      if (PQresultStatus(snapshot) != PGRES_COMMAND_OK) {
	fprintf(stderr, "TRUNCATE TABLE command failed: %s\n", PQerrorMessage(conn));
	PQclear(snapshot);
	MPI_Finalize();
	return -1;
      }
      PQclear(snapshot);
    }
    /* Set up signal handlers */
    signal (SIGURG, sig_user1);
    signal (SIGALRM, catch_alarm);
    /* Set alarm */
    alarm(WAIT_TIME);
    manager(beginning_path, nproc, restart_name, id);
  }
  else
    worker(rank);
  
  if (db_on) 
  {
    if (rank == 0) 
    {
      /* Insert root path */
      struct stat st;
      
      if(lstat(beginning_path, &st) != 0)
      {
          perror("main(), lstat()");
          MPI_Finalize();
          exit(-1);
      }
      char binary[80];
      char abslink[10] = "false";
      dec2bin(st.st_mode, binary);
      if ((S_ISLNK(st.st_mode)) && (beginning_path[0] == '/')) 
      {
        bzero(abslink, 10);
        snprintf(abslink, 5, "true");
      }
      else 
      {
        bzero(abslink, 10);
        snprintf(abslink, 6, "false");	      
      }
      char stat_query[1024];

      if (S_ISDIR(st.st_mode) && !(S_ISLNK(st.st_mode)))
          snprintf(stat_query, 1024, 
          "INSERT INTO %s (filename, parent, inode, mode, nlink, uid, gid, size, block, block_size, atime, mtime, ctime, abslink, root)" 
         " VALUES ('%s/', '%s', %ju, B'%s', %ju, %i, %i, %ju, %ju, %ju, timestamp without time zone 'epoch' + %ju * interval '1 second', timestamp without time zone 'epoch' + %ju * interval '1 second', timestamp without time zone 'epoch' + %ju * interval '1 second', %s,'true');", 
          snapshot_name, beginning_path, beginning_path, st.st_ino, binary, st.st_nlink, st.st_uid, st.st_gid, st.st_size, st.st_blocks, st.st_blksize, st.st_atime, st.st_mtime, st.st_ctime, abslink);

      PGresult  *insert_result = PQexec(conn, stat_query);
      if (PQresultStatus(insert_result) != PGRES_COMMAND_OK) 
      {
          fprintf(stderr, "INSERT INTO snapshot command failed (%s) for root: %s\n", beginning_path, PQerrorMessage(conn));
          fprintf(stderr, "%s\n", stat_query);
          /*MPI_Abort(MPI_COMM_WORLD, -1);*/
      }
      PQclear(insert_result);
      fprintf(stderr,"Inserted root: %s\n", beginning_path);
        
        if (strncmp(snapshot_name, "snapshot1", 16) == 0) {
	snapshot = PQexec(conn, "UPDATE current_snapshot SET name = 'snapshot1' WHERE ID = 1;");
	if (PQresultStatus(snapshot) != PGRES_COMMAND_OK) {
	  fprintf(stderr, "UPDATE current_snapshot command failed: %s\n", PQerrorMessage(conn));
	  PQclear(snapshot);
	  MPI_Finalize();
	  return -1;
	}
	PQclear(snapshot);
      } else {
	snapshot = PQexec(conn, "UPDATE current_snapshot SET name = 'snapshot2' WHERE ID = 1;");
	if (PQresultStatus(snapshot) != PGRES_COMMAND_OK) {
	  fprintf(stderr, "UPDATE current_snapshot command failed: %s\n", PQerrorMessage(conn));
	  PQclear(snapshot);
	  MPI_Finalize();
	  return -1;
	}
	PQclear(snapshot);
      }
      snapshot = PQexec(conn, "UPDATE current_snapshot SET updated = now() WHERE ID = 1;");
      if (PQresultStatus(snapshot) != PGRES_COMMAND_OK) {
	fprintf(stderr, "UPDATE current_snapshot,updated command failed: %s\n", PQerrorMessage(conn));
	PQclear(snapshot);
	MPI_Finalize();
	return -1;
      }
      PQclear(snapshot);
    }
    PQfinish(conn);
  }
  
  MPI_Finalize();
  
  return 0;
}


/***********************************************************************************************
* Function: manager (rank 0)  
*
* Description:  This function is responsible for the management of processes that walk 
* subdirectories.  It manages a queue of directories and names to process. 
* It sends work via MPI_Send to worker. 
*             
***********************************************************************************************/
void manager(char *beginning_path, int nproc, char *restart_name, int id)
{

  int i,j,k,l;
  int loop;
  int dmaxque = QSIZE;
  int dlastmaxque = QSIZE;
  int nmaxque = QSIZE;
  int nlastmaxque = QSIZE;
  char path[PATHSIZE_PLUS];
  char parent[PATHSIZE_PLUS];
  char abortcmd[10];
  int received_cmd = -1;
  int free_index;
  int setup_error = 0;
  int position = 0;
  int position_a = 0;
  int type_cmd;
  int count;
  int send_rank;
  int totaldirs = 0;
  int totalfiles = 0;
  int totalthings = 0;
  
  struct timeval tv;
  time_t now;
  int timenow=0;

  MPI_Status status;

  PGresult *res;
  char query[500];

  /*setup working buffer for MPI messages*/
  char* workbuf = (char*) malloc(WORKSIZE * sizeof(char));
  char* workbuf_a = (char*) malloc(WORKSIZE * sizeof(char));

  /*restart file pointer*/
  FILE *restart_file;
  char line[PATHSIZE_PLUS];
 
  /* pointer for req_queue which holds all the outstanding directories*/
  struct dir_queue {
     char req[PATHSIZE_PLUS];
  };
  struct dir_queue *dqueue;

  struct name_queue {
     char req[PATHSIZE_PLUS];
  };
  struct name_queue *nqueue;

  int all_done = 0;

  sprintf(abortcmd,"%d",ABORTCMD);

  
  //MPI_Pack_size(PATHSIZE_PLUS, MPI_CHAR, MPI_COMM_WORLD, &packedfname);
  //fprintf(stderr,"packed: %i worksize: %i pathsize: %i packsize: %i buff: %i\n", packedfname, WORKSIZE, PATHSIZE_PLUS, PACKSIZE, WORKSIZE * (int)sizeof(char));
  /* malloc space, set up proc_status one slot for every nproc */
  if ((proc_status = (int *)malloc(nproc*(sizeof(int)))) == (int *)0) {
     fprintf(stderr, "Error allocating memory: %s\n\n", "proc_status");
     send_abort_cmd(abortcmd, nproc);
     setup_error=1;
  }
  /* malloc space for dqueue */
  if ((dqueue = (struct dir_queue *)malloc(QSIZE*sizeof(struct dir_queue))) ==
                   (struct dir_queue *)0) {
     fprintf(stderr, "Error allocating memory: %s\n\n", "dir_queue");
     send_abort_cmd(abortcmd, nproc);
     setup_error=1;
  }
  /* malloc space for nqueue */
  if ((nqueue = (struct name_queue *)malloc(QSIZE*sizeof(struct name_queue))) ==
                   (struct name_queue *)0) {
     fprintf(stderr, "Error allocating memory: %s\n\n", "name_queue");
     send_abort_cmd(abortcmd, nproc);
     setup_error=1;
  }
  /*set restart if continuing from checkpoint*/
  if (restart) {
    restart_file = fopen(restart_name, "r");
    char * p = fgets(line, sizeof(line), restart_file);
    if(p == NULL)
        exit(EXIT_FAILURE);
    if(i < 0)
        exit(EXIT_FAILURE);
    if (line[strlen(line) - 1] == '\n') {
      line[strlen(line) - 1] = '\0';
    }
    nreqcnt = atoi(line);
    for (loop=0; loop<nreqcnt; loop++) {
        
      char * p = fgets(line, sizeof(line), restart_file);
      if(p == NULL)
          exit(EXIT_FAILURE);
      if (line[strlen(line) - 1] == '\n') {
	line[strlen(line) - 1] = '\0';
      }
      strncpy(nqueue[loop].req, line, PATHSIZE_PLUS);
    }
    p = fgets(line, sizeof(line), restart_file);
    if(p == NULL) exit(EXIT_FAILURE);
    if (line[strlen(line) - 1] == '\n') {
      line[strlen(line) - 1] = '\0';
    }
    dreqcnt = atoi(line);
    for (loop=0; loop<dreqcnt; loop++) {
      p = fgets(line, sizeof(line), restart_file);
      if(p == NULL) exit(EXIT_FAILURE);
      if (line[strlen(line) - 1] == '\n') {
        line[strlen(line) - 1] = '\0';
      }
      strncpy(dqueue[loop].req, line, PATHSIZE_PLUS);
    }
    fclose(restart_file);
  }

  /* Proceed only if no setup errors above. */
  if (setup_error == 0) {
    if (!restart) {
      memset (dqueue[0].req, '\0', PATHSIZE_PLUS);
      memset (nqueue[0].req, '\0', PATHSIZE_PLUS);
    }

    /* initialize all slots in proc_status to not busy */
    for (i = 0; i < nproc; i++) 
      proc_status[i]=0;
    
    /* mark proc=1 (directory reader) busy in proc_status */ 
    proc_status[1]=1;
    
    /* send message to proc=1 (directory reader) dircmd and beginning_path */
    type_cmd = DIRCMD;
    MPI_Pack(&type_cmd, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);

    if (restart) {
      MPI_Pack(&dqueue[dreqcnt-1].req, PATHSIZE_PLUS, MPI_CHAR, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
      strncpy(lastpath, dqueue[dreqcnt-1].req, PATHSIZE_PLUS);
      dreqcnt--;
    }
    else {
      MPI_Pack(beginning_path, PATHSIZE_PLUS, MPI_CHAR, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
      strncpy(lastpath, beginning_path, PATHSIZE_PLUS);
    }

    /* send first directory to reader. this gets things started*/
    if (MPI_Send (workbuf, position, MPI_PACKED, NAMEREAD_PROC, NAMEREAD_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) {
      fprintf(stderr, "ERROR when manager (rank 0) attempted to send top level directory to rank 1.\n");  
      MPI_Abort(MPI_COMM_WORLD, -1);
    }

    totaldirs += 1;
    totalthings += 1;
    
    /* initialize reqcnt */
    if (!restart) {
      dreqcnt = 0;
      nreqcnt = 0;
    }

    /* loop until all_done=1 */
    while (all_done == 0) {
      /* recv message from anyone */
      if (MPI_Recv(workbuf, WORKSIZE, MPI_PACKED, MPI_ANY_SOURCE, MPI_ANY_TAG, 
		   MPI_COMM_WORLD,&status) != MPI_SUCCESS) {
	fprintf(stderr, "ERROR in manager when receiving message.\n");  
	MPI_Abort(MPI_COMM_WORLD, -1);
      }
      
      position = 0;
      MPI_Unpack(workbuf, WORKSIZE, &position, &received_cmd, 1, MPI_INT, MPI_COMM_WORLD);
      
      switch (received_cmd)
	{
	  /* A DIRCMD says that a remote proc found a subdir, and we should give it to an available
	     reader or queue it up for when one becomes available*/
	case DIRCMD:
	  MPI_Unpack(workbuf, WORKSIZE, &position, &count, 1, MPI_INT, MPI_COMM_WORLD);
	  MPI_Unpack(workbuf, WORKSIZE, &position, &send_rank, 1, MPI_INT, MPI_COMM_WORLD);
	  //fprintf(stderr,"MANAGER sees %i dirs from %i.\n", count, send_rank);
	  totaldirs += count;
	  if (count>0) {
	    if (MPI_Recv(workbuf, WORKSIZE, MPI_PACKED, send_rank, MPI_ANY_TAG, 
			 MPI_COMM_WORLD,&status) != MPI_SUCCESS) {
	      fprintf(stderr, "ERROR in manager when receiving message.\n");  
	      MPI_Abort(MPI_COMM_WORLD, -1);
	    }
	    position = 0;
	  }
	  
	  if ((count > 0) && (proc_status[1] == 0)) {
	    //fprintf(stderr,"reader is available, sending dir\n");
	    MPI_Unpack(workbuf, WORKSIZE, &position, &path, PATHSIZE_PLUS, MPI_CHAR, MPI_COMM_WORLD);
	    type_cmd = DIRCMD;
	    position_a = 0;
	    MPI_Pack(&type_cmd, 1, MPI_INT, workbuf_a, WORKSIZE, &position_a, MPI_COMM_WORLD);
	    MPI_Pack(path, PATHSIZE_PLUS, MPI_CHAR, workbuf_a, WORKSIZE, &position_a, MPI_COMM_WORLD);
	    //fprintf(stderr,"sending %s to reader.\n", path);

	    strncpy(lastpath, path, PATHSIZE_PLUS);

	    if (MPI_Send(workbuf_a, position_a, MPI_PACKED, NAMEREAD_PROC, 
			 NAMEREAD_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) {
	      fprintf(stderr, "ERROR in manager sending when sending dir to reader.\n");  
	      MPI_Abort(MPI_COMM_WORLD, -1);
	    }
	    
	    /* mark the assigned proc busy in proc_status */
	    proc_status[1] = 1;
	    j = 1;
	  }
	  else if ((count == 0) && (proc_status[1] == 0) && (dreqcnt > 0) && (!shutdown)) {
	    fprintf(stderr,"THIS SHOULDNT HAPPEN, ONLY HERE FOR WHEN MORE READERS\n");
	    type_cmd = DIRCMD;
	    position = 0;
	    MPI_Pack(&type_cmd, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
	    MPI_Pack(&dqueue[dreqcnt-1].req, PATHSIZE_PLUS, MPI_CHAR, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);

	    strncpy(lastpath, dqueue[dreqcnt-1].req, PATHSIZE_PLUS);
	    
	    if (MPI_Send (workbuf, position, MPI_PACKED, 
			  NAMEREAD_PROC, NAMEREAD_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) {
	      fprintf(stderr, "ERROR in manager sending when sending message to slave.\n");  
	      MPI_Abort(MPI_COMM_WORLD, -1);
	    }
	    
	    /* mark the assigned proc busy in proc_status */
	    /* mark free proc busy in proc_status */
	    //fprintf(stderr,"marking %i as unavailable\n", i);
	    proc_status[i] = 1;
	    
	    /* remove work slot from req_queue */
	    dreqcnt--;
	  }
	  else {
	    //fprintf(stderr,"reader is not available\n");
	    j = 0;
	  }

	  for (; j < count; j++) {
	    MPI_Unpack(workbuf, WORKSIZE, &position, &path, PATHSIZE_PLUS, MPI_CHAR, MPI_COMM_WORLD);
	    
	    //fprintf(stderr,"queueing dir %s\n", path);

	    /* put dir on dqueue */
	    sprintf(dqueue[dreqcnt].req,"%s",path);
	    dreqcnt++;
	    if (dreqcnt > dmaxque) {
	      dmaxque+=100;
	      if ((dqueue = (struct dir_queue *)realloc((void *)dqueue,(dmaxque + 1)*(sizeof(struct dir_queue)))) == (struct dir_queue *)0)
		MPI_Abort(MPI_COMM_WORLD, -1);

	      for (k = dlastmaxque + 1; k < (dmaxque + 1); k++)
		memset(dqueue[k].req, '\0', PATHSIZE_PLUS);
	      
	      dlastmaxque = dmaxque;
	    } /* end if avaiable worker */
	  }

	  break;
	  
	  /* A NAMECMD says that a remote proc has a list of names for us to put on the stat queue, 
	     and we should give it to an available stat worker or queue it up for when one becomes 
	     available*/
	case NAMECMD:
	  MPI_Unpack(workbuf, WORKSIZE, &position, &count, 1, MPI_INT, MPI_COMM_WORLD);
	  MPI_Unpack(workbuf, WORKSIZE, &position, &send_rank, 1, MPI_INT, MPI_COMM_WORLD);

	  totalthings += count;
	  
	  if (count>0) 
      {
	    if (MPI_Recv(workbuf, WORKSIZE, MPI_PACKED, send_rank, MPI_ANY_TAG, 
			 MPI_COMM_WORLD,&status) != MPI_SUCCESS) 
        {
	      fprintf(stderr, "ERROR in manager when receiving message.\n");  
	      MPI_Abort(MPI_COMM_WORLD, -1);
	    }
	    position = 0;
	  }
	  else
      {
	      break;
      }	  
	  /* if there is a free stat-er proc in proc_status, send it the list */
	  if ((free_index = get_proc_status(proc_status, nproc, 2)) != -1) 
      {
	    if (count < MAX_STAT) 
        {
	      type_cmd = NAMECMD;
	      position_a = 0;
	      MPI_Pack(&type_cmd, 1, MPI_INT, workbuf_a, WORKSIZE, &position_a, MPI_COMM_WORLD);
	      MPI_Pack(&count, 1, MPI_INT, workbuf_a, WORKSIZE, &position_a, MPI_COMM_WORLD);

	      for (k = 0; k < count; k++) 
          {
             MPI_Unpack(workbuf, WORKSIZE, &position, &path, PATHSIZE_PLUS, MPI_CHAR, MPI_COMM_WORLD);
             MPI_Pack(path, PATHSIZE_PLUS, MPI_CHAR, workbuf_a, WORKSIZE, &position_a, MPI_COMM_WORLD);
            //fprintf(stderr,"%i packing: %s\n", MANAGER_PROC, path);
	      }

	      //fprintf(stderr,"%i sending list of %i names to stat-er(%i).\n", MANAGER_PROC, count, free_index);
	      
          /* If the beginning_path (root path) is included in this workbuffer, we flag it */
           if (MPI_Send(workbuf_a, position_a, MPI_PACKED, free_index, 
			   free_index, MPI_COMM_WORLD ) != MPI_SUCCESS) 
          {
                fprintf(stderr, "ERROR in manager sending when sending dir to reader.\n");  
                MPI_Abort(MPI_COMM_WORLD, -1);
	      }
	      
	      /* mark the assigned proc busy in proc_status */
	      proc_status[free_index] = 1;
	    }
	    else 
        {
	      type_cmd = NAMECMD;
	      position_a = 0;
	      MPI_Pack(&type_cmd, 1, MPI_INT, workbuf_a, WORKSIZE, &position_a, MPI_COMM_WORLD);
	      MPI_Pack(&count, 1, MPI_INT, workbuf_a, WORKSIZE, &position_a, MPI_COMM_WORLD);

	      for (k = 0; k < MAX_STAT; k++) 
          {
                MPI_Unpack(workbuf, WORKSIZE, &position, &path, PATHSIZE_PLUS, MPI_CHAR, MPI_COMM_WORLD);
                MPI_Pack(path, PATHSIZE_PLUS, MPI_CHAR, workbuf_a, WORKSIZE, &position_a, MPI_COMM_WORLD);
	      }
          if (MPI_Send(workbuf_a, position_a, MPI_PACKED, free_index, 
			   free_index, MPI_COMM_WORLD ) != MPI_SUCCESS) 
          {
                fprintf(stderr, "ERROR in manager sending when sending dir to reader.\n");  
                MPI_Abort(MPI_COMM_WORLD, -1);
	      }
	      
	      /* mark the assigned proc busy in proc_status */
	      proc_status[free_index] = 1;
	      
	      for (; k < count; k++) 
          {
            MPI_Unpack(workbuf, WORKSIZE, &position, &path, PATHSIZE_PLUS, MPI_CHAR, MPI_COMM_WORLD);
            
            /* put rest of list on nqueue */
            sprintf(nqueue[nreqcnt].req,"%s",path);
            nreqcnt = nreqcnt + 1;
            if (nreqcnt > nmaxque) 
            {
              nmaxque+=100;
              if ((nqueue = (struct name_queue *)realloc((void *)nqueue,(nmaxque + 1)*(sizeof(struct name_queue)))) == (struct name_queue *)0)
                MPI_Abort(MPI_COMM_WORLD, -1);

              for (l = nlastmaxque + 1; l < (nmaxque + 1); l++)
                memset(nqueue[l].req, '\0', PATHSIZE_PLUS);
              
              nlastmaxque = nmaxque;
            } /* end if */
	      } /* end for */
	    } /* end else */
	  }	/* end if */ 
	  else 
      {
	    for (k = 0; k < count; k++) 
        {
	      MPI_Unpack(workbuf, WORKSIZE, &position, &path, PATHSIZE_PLUS, MPI_CHAR, MPI_COMM_WORLD);

	      /* put dir on nqueue */
	      sprintf(nqueue[nreqcnt].req,"%s",path);
	      nreqcnt = nreqcnt + 1;
	      if (nreqcnt > nmaxque) 
          {
            nmaxque+=100;
            if ((nqueue = (struct name_queue *)realloc((void *)nqueue,(nmaxque + 1)*(sizeof(struct name_queue)))) == (struct name_queue *)0)
              MPI_Abort(MPI_COMM_WORLD, -1);
		
            for (l = nlastmaxque + 1; l < (nmaxque + 1); l++)
              memset(nqueue[l].req, '\0', PATHSIZE_PLUS);
		
            nlastmaxque = nmaxque;
	      } /* end if */
	    } /* end if avaiable worker */
	  } /* end else (queue it)*/

	  break; /* the never ending NAMECMD case */
	  
	  /* if this is a workreq */
	  /* This signifies that the slave has completed task and proc_status[] can be updated to 
	     reflect this and the req_queue can be searched for more work */
	case REQCMD:
	  MPI_Unpack(workbuf, WORKSIZE, &position, &i, 1, MPI_INT, MPI_COMM_WORLD);
	  /* mark this worker free in the proc_status */
	  //fprintf(stderr,"marking %i as available\n", i);
	  proc_status[i] = 0;
	  
	  j = 0;

	  for (k = 1; k < nproc; k++)
	    j = proc_status[k] + j;

	  //fprintf(stderr,"i: %i  d: %i  n: %i  j: %i\n", i, dreqcnt, nreqcnt, j);
	  
	  if ((dreqcnt == 0) && (nreqcnt == 0) && (j == 0)) {
	    /* everyone is done, send exitcmd to all workers */
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

	    totalfiles = totalthings - totaldirs;

	    if (db_on) {
	      gettimeofday(&tv, NULL);
	      now = tv.tv_sec;
	      timenow = (int)now;

	      snprintf(query, 500,"UPDATE performance SET files=%i, directories=%i, runtime=%i, ranks=%i, packsize=%i, error=0 WHERE id = %i;", totalfiles, totaldirs, (timenow - starttime), nproc, PACKSIZE, id); 
	      res = PQexec(conn, query);
	      if (PQresultStatus(res) != PGRES_COMMAND_OK) {
		fprintf(stderr, "UPDATE command failed: %s\n", PQerrorMessage(conn));
		PQclear(res);
	      }
	      
	      PQclear(res);

          }
	    else
	      fprintf(stderr,"\ntotalfiles: %i totaldirs: %i\n", totalfiles, totaldirs);
	    
	    all_done = 1;
	  }
	  else if (shutdown) {
	    /*in shutdown phase waiting for all procs to quiesce, don't give out work*/
	    continue;
	  }	  
	  /*reader requesting work?*/
	  /* if there is some work on the req_queue */
	  /* add logic here to pause the reader if its getting too far ahead*/
	  else if ((i == 1) && (dreqcnt > 0) && (nreqcnt > MAX_NCOUNT)) 
      {
	    type_cmd = WAITCMD;
	    position = 0;
	    MPI_Pack(&type_cmd, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
	    if (MPI_Send (workbuf, position, MPI_PACKED, 
			  NAMEREAD_PROC, NAMEREAD_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) 
        {
	      fprintf(stderr, "ERROR in manager sending when sending message to slave.\n");  
	      MPI_Abort(MPI_COMM_WORLD, -1);
	    }
	    
	    /* mark the assigned proc busy in proc_status */
	    /* mark free proc busy in proc_status */
	    //fprintf(stderr,"marking %i as unavailable\n", i);
	    proc_status[i] = 1;
	  }
	  else if ((i == 1) && (dreqcnt > 0)) {
	    type_cmd = DIRCMD;
	    position = 0;
	    MPI_Pack(&type_cmd, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
	    MPI_Pack(&dqueue[dreqcnt-1].req, PATHSIZE_PLUS, MPI_CHAR, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);

	    strncpy(lastpath, dqueue[dreqcnt-1].req, PATHSIZE_PLUS);
	    
	    if (MPI_Send (workbuf, position, MPI_PACKED, 
			  NAMEREAD_PROC, NAMEREAD_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) {
	      fprintf(stderr, "ERROR in manager sending when sending message to slave.\n");  
	      MPI_Abort(MPI_COMM_WORLD, -1);
	    }
	    
	    /* mark the assigned proc busy in proc_status */
	    /* mark free proc busy in proc_status */
	    //fprintf(stderr,"marking %i as unavailable\n", i);
	    proc_status[i] = 1;
	    
	    /* remove work slot from req_queue */
	    dreqcnt--;
	  }
	  else if ((i > 1) && (nreqcnt > 0)) {
	    type_cmd = NAMECMD;
	    position = 0;

	    MPI_Pack(&type_cmd, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);

	    if (nreqcnt > MAX_STAT) {
	      count = MAX_STAT;
	      MPI_Pack(&count, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
	      
	      for (k = 0; k < MAX_STAT; k++) {
		MPI_Pack(&nqueue[nreqcnt-1].req, PATHSIZE_PLUS, MPI_CHAR, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
		nreqcnt--;
	      }

	      //fprintf(stderr,"%i sending list of %i names to stat-er(%i).\n", MANAGER_PROC, count, i);
	      if (MPI_Send (workbuf, position, MPI_PACKED, i, i, MPI_COMM_WORLD ) != MPI_SUCCESS) {
		fprintf(stderr, "ERROR in manager sending when sending message to slave.\n");  
		MPI_Abort(MPI_COMM_WORLD, -1);
	      }
	    }
	    else {
	      count = nreqcnt;
	      MPI_Pack(&count, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
	      
	      for (k = 0; k < count; k++) {
		MPI_Pack(&nqueue[nreqcnt-1].req, PATHSIZE_PLUS, MPI_CHAR, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
		nreqcnt--;
	      }
	      
	      //fprintf(stderr,"%i sending list of %i names to stat-er(%i).\n", MANAGER_PROC, count, i);
	      if (MPI_Send (workbuf, position, MPI_PACKED, i, i, MPI_COMM_WORLD ) != MPI_SUCCESS) {
		fprintf(stderr, "ERROR in manager sending when sending message to slave.\n");  
		MPI_Abort(MPI_COMM_WORLD, -1);
	      }
	    }	      
	    
	    /* mark the assigned proc busy in proc_status */
	    /* mark free proc busy in proc_status */
	    //fprintf(stderr,"marking %i as unavailable\n", i);
	    proc_status[i] = 1;
	    
	    /* remove work slot from req_queue */
	  }
	  break;
	} /*switch received command*/
    } /*while not all done*/
    /*stderr? this should be in an output log or something probably*/
    //fprintf(stderr,"maximum ever in queue %d\n",maxque);
    //fprintf(stderr,"Tree walked in %ju seconds.\n",out.tv_sec-in.tv_sec);
  } /* endif setup_error */
  
  if (shutdown) { /*write a restart file*/
    restart_file = fopen("treewalk_restart", "w+");
    fprintf(restart_file,"%i\n", nreqcnt);
    for (loop=0; loop<nreqcnt; loop++)
      fprintf(restart_file,"%s\n", nqueue[loop].req);
    fprintf(restart_file,"%i\n", dreqcnt);
    for (loop=0; loop<dreqcnt; loop++)
      fprintf(restart_file,"%s\n", dqueue[loop].req);
    fclose(restart_file);
  }

  /*cleanup memory*/
  free(proc_status);
  free(dqueue);
  free(nqueue);
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
  char path[PATHSIZE_PLUS] = "";
  char parent[PATHSIZE_PLUS] = "";
  char statpath[PATHSIZE_PLUS];
  MPI_Status status;
  DIR *dir;
  struct dirent *ent;
  struct stat st;
  int position=0;
  int position_a=0;
  int type_cmd;
  int abort=0;
  int count=0;
  int count_in=0;
  int k;
  int root = 0;

  PGresult *insert_result;
  char stat_query[1024];
  char binary[80];
  char abslink[10] = "false";

  /*initialize MPI send/recv buffer*/
  char* workbuf = (char*) malloc(WORKSIZE * sizeof(char));
  char* workbuf_a = (char*) malloc(WORKSIZE * sizeof(char));

  while (all_done == 0) 
  {
    /*get our next task*/
    if (MPI_Recv(workbuf, WORKSIZE, MPI_PACKED, MPI_ANY_SOURCE, MPI_ANY_TAG,MPI_COMM_WORLD,&status) != MPI_SUCCESS) 
    {
      fprintf(stderr, "ERROR in worker receiving.\n");  
      MPI_Abort(MPI_COMM_WORLD, -1);
    }
    
    position = 0;
    MPI_Unpack(workbuf, WORKSIZE, &position, &type_cmd, 1, MPI_INT, MPI_COMM_WORLD);
    
    switch(type_cmd) 
    {
          /*if we get an ABORT, lets do so*/
        case ABORTCMD:
          abort=1;
          break;
        
          /*a DIRCMD means we get a directory to work on*/
        case DIRCMD:
          MPI_Unpack(workbuf, WORKSIZE, &position, &path, PATHSIZE_PLUS, MPI_CHAR, MPI_COMM_WORLD);
          
          //fprintf(stderr,"dir: %s\n", path);
          /*open the directory that was sent to us*/
          dir = opendir(path);
          
          if (!dir) 
          {
            fprintf(stderr,"%i: %s\n", rank, path);
            perror("pstat.c, worker(), opendir()");
          } 
          else 
          {
            position_a = 0;
            count = 0;
        
            /* Read in directories */
            while ((ent = readdir(dir)) != NULL) 
            {
                  if ((strncmp(ent->d_name,".",10)) && (strncmp(ent->d_name,"..",10))) 
                  {
                    /* use strcat to build directory path because directory name can have spaces */
                    /*FIXME ** WARNING: hardcoded filename length in here!*/
                    /*FIXME ** WARNING: buffer overflow capability by 1025!!!*/
                    strncpy(statpath, path, PATHSIZE_PLUS);
                    strncat(statpath,"/",1);
                    strncat(statpath,ent->d_name,1024);
                    
                    //fprintf(stderr,"%i packing: %s\n", rank, statpath);
                    //fprintf(stderr,"%i count: %i\n", rank, count);
                    MPI_Pack(&statpath, PATHSIZE_PLUS, MPI_CHAR, workbuf_a, WORKSIZE, &position_a, MPI_COMM_WORLD);
                    count++;
                    //fprintf(stderr,"%i: count - %i %s\n", rank, count, statpath);

                    if ((count % PACKSIZE) == 0) 
                    {
                      type_cmd = NAMECMD;
                      position = 0;
                      MPI_Pack(&type_cmd, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
                      MPI_Pack(&count, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
                      MPI_Pack(&rank, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
                      
                      //fprintf(stderr, "%i sending list of %i names back to manager.\n", rank, count);

                      if (MPI_Send(workbuf, position, MPI_PACKED, MANAGER_PROC, MANAGER_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) 
                      {
                        fprintf(stderr, "ERROR in worker sending message.\n");  
                        MPI_Abort(MPI_COMM_WORLD, -1);
                      }
                      if (count>0) 
                      {
                        if (MPI_Send(workbuf_a, position_a, MPI_PACKED, MANAGER_PROC, MANAGER_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) 
                        {
                          fprintf(stderr, "ERROR in worker sending message.\n");  
                          MPI_Abort(MPI_COMM_WORLD, -1);
                        }
                      }
                      count = 0;
                      position_a = 0;
                    }
                  }
            } /* end while reading dir */
        
        if (closedir(dir) == -1) 
        {
          fprintf(stderr,"%i: %s\n", rank, path);
          perror("pstat.c, worker(), closedir()");
        }
        
      } /* if opendir worked */
         
          type_cmd = NAMECMD;
          position = 0;
          MPI_Pack(&type_cmd, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
          MPI_Pack(&count, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
          MPI_Pack(&rank, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);

          //fprintf(stderr, "%i sending list of %i names back to manager.\n", rank, count);

          if (MPI_Send(workbuf, position, MPI_PACKED, MANAGER_PROC, MANAGER_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) 
          {
            fprintf(stderr, "ERROR in worker sending message.\n");  
            MPI_Abort(MPI_COMM_WORLD, -1);
          }
          if (count>0) 
          {
            if (MPI_Send(workbuf_a, position_a, MPI_PACKED, MANAGER_PROC, MANAGER_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) 
            {
              fprintf(stderr, "ERROR in worker sending message.\n");  
              MPI_Abort(MPI_COMM_WORLD, -1);
            }
          }
         
          /* done with the directory, send workreq to manager to get more work or finish up */
          type_cmd = REQCMD;
          position = 0;
          MPI_Pack(&type_cmd, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
          MPI_Pack(&rank, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);

          //fprintf(stderr,"%i sending REQCMD to manager\n", rank);
          
          if (MPI_Send(workbuf, position, MPI_PACKED, MANAGER_PROC, MANAGER_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) 
          {
            fprintf(stderr, "ERROR in worker sending message.\n");  
            MPI_Abort(MPI_COMM_WORLD, -1);
          }
          break;
          
        case NAMECMD:
          
          if(status.MPI_TAG = NAMECMD_CHKROOT)
              root = 1;
          MPI_Unpack(workbuf, WORKSIZE, &position, &count_in, 1, MPI_INT, MPI_COMM_WORLD);
          count = 0;
          position_a = 0;

          //fprintf(stderr, "%i count_in: %i\n", rank, count_in);
          for (k = 0; k < count_in; k++) 
          {
              MPI_Unpack(workbuf, WORKSIZE, &position, &path, PATHSIZE_PLUS, MPI_CHAR, MPI_COMM_WORLD);
              /* use lstat for link checking */
             if (lstat(path,&st) == -1) 
             {
                  fprintf(stderr,"%s\n",path);
                  perror("pstat.c, worker(), lstat()");
             } 
             else 
             { 
                  /* Check if directory and not a link */
                  if (db_on) 
                  {
                    dec2bin(st.st_mode, binary);
                    if ((S_ISLNK(st.st_mode)) && (path[0] == '/')) 
                    {
                      bzero(abslink, 10);
                      snprintf(abslink, 5, "true");
                    }
                   else 
                   {
                      bzero(abslink, 10);
                      snprintf(abslink, 6, "false");	      
                   }
                /* Get parent */
                 split_path(parent,path,PATHSIZE_PLUS); 
                    if (S_ISDIR(st.st_mode) && !(S_ISLNK(st.st_mode)))
                      snprintf(stat_query, 1024, "INSERT INTO %s (filename, parent, inode, mode, nlink, uid, gid, size, block, block_size, atime, mtime, ctime, abslink) VALUES ('%s/', '%s', %ju, B'%s', %ju, %i, %i, %ju, %ju, %ju, timestamp without time zone 'epoch' + %ju * interval '1 second', timestamp without time zone 'epoch' + %ju * interval '1 second', timestamp without time zone 'epoch' + %ju * interval '1 second', %s);", snapshot_name, path, parent, st.st_ino, binary, st.st_nlink, st.st_uid, st.st_gid, st.st_size, st.st_blocks, st.st_blksize, st.st_atime, st.st_mtime, st.st_ctime, abslink);
                    else
                      snprintf(stat_query, 1024, "INSERT INTO %s (filename, parent, inode, mode, nlink, uid, gid, size, block, block_size, atime, mtime, ctime, abslink) VALUES ('%s', '%s', %ju, B'%s', %ju, %i, %i, %ju, %ju, %ju, timestamp without time zone 'epoch' + %ju * interval '1 second', timestamp without time zone 'epoch' + %ju * interval '1 second', timestamp without time zone 'epoch' + %ju * interval '1 second', %s);", snapshot_name, path, parent, st.st_ino, binary, st.st_nlink, st.st_uid, st.st_gid, st.st_size, st.st_blocks, st.st_blksize, st.st_atime, st.st_mtime, st.st_ctime, abslink);
                
                insert_result = PQexec(conn, stat_query);
                
                if (PQresultStatus(insert_result) != PGRES_COMMAND_OK) 
                {
                  fprintf(stderr, "INSERT INTO snapshot command failed (%s): %s\n", path, PQerrorMessage(conn));
                  fprintf(stderr, "%s\n", stat_query);
                  /*MPI_Abort(MPI_COMM_WORLD, -1);*/
                }
                PQclear(insert_result);
            
                /* 
                   snprintf(stat_query, 1024, "select merge(text '%s', %i, timestamp without time zone 'epoch' + %ju * interval '1 second', timestamp without time zone 'epoch' + %ju * interval '1 second', timestamp without time zone 'epoch' + %ju * interval '1 second');", path, st.st_uid, st.st_atime, st.st_mtime, st.st_ctime);
                
                insert_result = PQexec(conn, stat_query);
                
                if (PQresultStatus(insert_result) != PGRES_COMMAND_OK) {
                  //fprintf(stderr, "merge command failed (%s): %s\n", path, PQerrorMessage(conn));
                }
                PQclear(insert_result);
                */
           }
          if (S_ISDIR(st.st_mode) && !(S_ISLNK(st.st_mode))) 
          {
            //fprintf(stderr,"%i packing: %s\n", rank, path);
            //fprintf(stderr,"%i count: %i\n", rank, count);
            MPI_Pack(&path, PATHSIZE_PLUS, MPI_CHAR, workbuf_a, WORKSIZE, &position_a, MPI_COMM_WORLD);
            count++;
            if ((count % PACKSIZE) == 0) 
            {
              type_cmd = DIRCMD;
              position = 0;
              MPI_Pack(&type_cmd, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
              MPI_Pack(&count, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
              MPI_Pack(&rank, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
              
              //fprintf(stderr, "%i sending list of %i names back to manager.\n", rank, count);
              
              if (MPI_Send(workbuf, position, MPI_PACKED, MANAGER_PROC, MANAGER_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) 
              {
                fprintf(stderr, "ERROR in worker sending message.\n");  
                MPI_Abort(MPI_COMM_WORLD, -1);
              }
              if (count>0) 
              {
                if (MPI_Send(workbuf_a, position_a, MPI_PACKED, MANAGER_PROC, MANAGER_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) 
                {
                  fprintf(stderr, "ERROR in worker sending message.\n");  
                  MPI_Abort(MPI_COMM_WORLD, -1);
                }
              }
              count = 0;
              position_a = 0;
            }
          } 
          else 
          {
            /*we hit a file, so do we need to do something?*/
          }
        }
  }
          
          
          type_cmd = DIRCMD;
          position = 0;
          MPI_Pack(&type_cmd, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
          MPI_Pack(&count, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
          MPI_Pack(&rank, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
          
          //fprintf(stderr, "%i sending list of %i dirs back to manager.\n", rank, count);
          
          if (MPI_Send(workbuf, position, MPI_PACKED, MANAGER_PROC, MANAGER_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) {
        fprintf(stderr, "ERROR in worker sending message.\n");  
        MPI_Abort(MPI_COMM_WORLD, -1);
          }
          
          if (count>0) {
        if (MPI_Send(workbuf_a, position_a, MPI_PACKED, MANAGER_PROC, MANAGER_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) {
          fprintf(stderr, "ERROR in worker sending message.\n");  
          MPI_Abort(MPI_COMM_WORLD, -1);
        }
          }
          
          /* done with the list, send workreq to manager to get more work or finish up */
          type_cmd = REQCMD;
          position = 0;
          MPI_Pack(&type_cmd, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
          MPI_Pack(&rank, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
          
          if (MPI_Send(workbuf, position, MPI_PACKED, MANAGER_PROC, MANAGER_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) {
        fprintf(stderr, "ERROR in dodir sending message.\n");  
        MPI_Abort(MPI_COMM_WORLD, -1);
          }
          break;
          
          /*if we get an EXITCMD that means that all work is done, and we just need to update the collector with 
        the stats that we have been collecting*/
        case EXITCMD:
          all_done=1;
          break;
          
          /* WAITCMD means the queues are filling and we should just chill a little while */
        case WAITCMD:
          sleep(5);
          /* done with the wait, send workreq to manager to get more work or finish up */
          type_cmd = REQCMD;
          position = 0;
          MPI_Pack(&type_cmd, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
          MPI_Pack(&rank, 1, MPI_INT, workbuf, WORKSIZE, &position, MPI_COMM_WORLD);
          
          if (MPI_Send(workbuf, position, MPI_PACKED, MANAGER_PROC, MANAGER_TAG, MPI_COMM_WORLD ) != MPI_SUCCESS) {
        fprintf(stderr, "ERROR in worker sending message.\n");  
        MPI_Abort(MPI_COMM_WORLD, -1);
          }
          break;
        }
    
    if (abort == 1) break;
  }
  
  /*cleanup memory allocation*/  
  free(workbuf);
}
