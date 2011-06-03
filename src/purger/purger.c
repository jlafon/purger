#include "purger.h"

int main(int argc, char *argv[]){

  PGconn    *conn;
  PGresult  *uids;
  char 	     ins_timenow[100];
  int 	     i;
  char       filesystem[1024];
  dbinfo_t   dbinfo;
  ldapinfo_t ldapinfo;
  mailinfo_t mailinfo;
  time_t     mytime = time(NULL);
  int        option_index = 0;
  int        c;
  int        nopurge = 0;
  int        purgeonly = 0;

  static struct option long_options[] = {
    {"force",     no_argument, 0, 'f'},
    {"nopurge",   no_argument, 0, 'n'},
    {"purgeonly", no_argument, 0, 'p'},
    {"help",      no_argument, 0, 'h'},
    {0,0,0,0}
  };
  
  
  while ((c = getopt_long(argc, argv, "fnph", long_options, &option_index)) != -1) {
    switch (c) {
    case 'f':
      force = 1;
      break;
    case 'n':
      nopurge = 1;
      break;
    case 'p':
      purgeonly = 1;
      break;
    case 'h':
      usage(0);
      return(0);
      break;
    default:
      return(0);
    }
  }
  
  if (argc == optind) {
    PURGER_ELOG("main()", "%s", "usage: purger <filesystem>\nsee -h for options");
    return EXIT_FAILURE;
  }
  else if (strncpy(filesystem, argv[optind], 1024)==NULL){
    PURGER_ELOG("main()", "error initializing filesystem: %s", strerror(errno));
    return EXIT_FAILURE;
  }
  
  if (parse_config(&dbinfo, &ldapinfo, &mailinfo)==-1){
    PURGER_ELOG("main()", "%s returned error.", "parse_config()");
    return EXIT_FAILURE;
  }
  
  conn = PQsetdbLogin(dbinfo.host, dbinfo.port, NULL, NULL, filesystem, dbinfo.user, dbinfo.pass);
  
  if (PQstatus(conn) != CONNECTION_OK) {
    PURGER_ELOG("main()", "Connection to database failed: %s",
		PQerrorMessage(conn));
    exit_nicely(conn);
  }
  
  uids = PQexec(conn, "SELECT DISTINCT(uid) FROM expired_files;");
  if (PQresultStatus(uids) != PGRES_TUPLES_OK) {
    PURGER_ELOG("main()", "SELECT DISTINCT(uid) command failed: %s", 
		PQerrorMessage(conn));
    PQclear(uids);
    exit_nicely(conn);
  }
  
  if (strftime(ins_timenow, 100, "%Y-%m-%d %H:%M:%S", localtime(&mytime)) == 0) {
    PURGER_ELOG("main()", "%s", "strftime returned 0");
    PQclear(uids);
    exit_nicely(conn);
  }
  
  for (i = 0; i < PQntuples(uids); i++) {
    PURGER_LOG("main()", "uid: %s (%i of %i)", PQgetvalue(uids, i, 0), i+1, PQntuples(uids));
    if(!nopurge && (atoi(PQgetvalue(uids, i, 0)) != 0))
      if (process_warned_files(conn, PQgetvalue(uids, i, 0), filesystem, ins_timenow) != 0) {
	PURGER_ELOG("main()", "process_warned_files for uid %s returned non-success", PQgetvalue(uids, i, 0));
      }
    if(!purgeonly)
      if (process_unwarned_files(conn, PQgetvalue(uids, i, 0), filesystem, ins_timenow, &ldapinfo, &mailinfo) != 0) {
	PURGER_ELOG("main()", "process_uwarned_files for uid %s returned non-success", PQgetvalue(uids, i, 0));
      }
  }
  
  PQclear(uids);
  
  /* close the connection to the database and cleanup */
  PQfinish(conn);
  
  return 0;
}

static void usage() {
  printf("\npurger:\n");
  printf("Use the database to determine what files need to have notifications sent out and\n");
  printf("which can be purged.\n");
  printf("Usage:  purger [--force[-f]] [--nopurge[-n]] [--help[-h]] <filesystem>\n");
  printf("--force -f      Force serial purging\n");
  printf("--nopurge -n    Notifications only, no deletions\n");
  printf("--purgeonly -p  Deletions only, no notifications\n");
  printf("--help -h       Display help\n");
  printf("<filesystem>    which filesystem to purge\n\n");
  return;
}

static void exit_nicely(PGconn *conn) {
  PQfinish(conn);
  exit(EXIT_FAILURE);
}

int process_unwarned_files(PGconn *conn, char *uid, char *filesystem, char *ins_timenow, ldapinfo_t *ldapinfo, mailinfo_t * mailinfo){
  /* postgrs variables */
  PGresult *res, *files;
  PQprintOpt options = {0};
  char moniker[256];
  char notefile[256];

  /* string variables */
  char files_query[200], update_query[200];

  /* output file */
  FILE* outfile;

  /* Mail status */
  int mailerr=0;

  /* Time struct */
  time_t rawtime;
  
  snprintf(files_query, 200, "SELECT filename FROM expired_files WHERE uid = %s AND filename like '/panfs/%s/vol%%/%%/_%%' AND filename NOT like '/panfs/%s/vol%%/.panfs_store' AND warned = False;", uid, filesystem, filesystem);
  files = PQexec(conn, files_query);
  if (PQresultStatus(files) != PGRES_TUPLES_OK) {
    PURGER_ELOG("process_unwarned_files()", "SELECT * command failed: %s", PQerrorMessage(conn));
    PQclear(files);
    exit_nicely(conn);
  }      

  PURGER_LOG("process_unwarned_files()", "processing %i unwarned files for uid=%s... ", PQntuples(files), uid);
  
  if (PQntuples(files) == 0) {
    snprintf(update_query, 200, "UPDATE expired_files SET warned = True, added = '%s' WHERE warned = False AND uid = %s;", ins_timenow, uid);
    res = PQexec(conn, update_query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      PURGER_ELOG("process_unwarned_files()", "update warned command failed: %s", PQerrorMessage(conn));
      PQclear(res);
      exit_nicely(conn);
    }
  }  
  
  snprintf(files_query, 200, "SELECT filename FROM expired_files WHERE uid = %s AND filename like '/panfs/%s/vol%%/%%/_%%' AND filename NOT like '/panfs/%s/vol%%/.panfs_store';", uid, filesystem, filesystem);
  files = PQexec(conn, files_query);
  if (PQresultStatus(files) != PGRES_TUPLES_OK) {
    PURGER_ELOG("process_unwarned_files()", "SELECT * command failed: %s", PQerrorMessage(conn));
    PQclear(files);
    exit_nicely(conn);
  }      
  
  /* grab moniker from uid */
  if (strncmp(uid, "0", 2) == 0) {
    snprintf(moniker, 5, "nfs");
    snprintf(notefile, 256, "/var/log/purger/expired-files-root-%s.txt", filesystem);
  }
  else if (get_moniker( uid, ldapinfo->host, ldapinfo->basem, moniker ) == 1) {
    PURGER_ELOG("process_unwarned_files()", "Error getting moniker from ldap host: %s base: %s uid: %s", ldapinfo->host, ldapinfo->basem, uid);
    /* UID DOESN'T EXIST? SET ALL FILES TO WARNED?  WHERE TO PUT THE NOTIFICATION FILE? */
    snprintf(notefile, 256, "/var/log/purger/lostuids/%s-%s.txt", filesystem, uid);
    moniker[0]='\0';
  }
  else
    snprintf(notefile, 256, "/%s/%s/expired-files.txt", filesystem, moniker);
  
  if (PQntuples(files) == 0) {
    remove(notefile);
    return 0;
  }

  PURGER_LOG("process_unwarned_files()", "opening file: %s", notefile);
  
  outfile = fopen(notefile, "w");
  
  if (!outfile)
    perror("error opening expired file");
  else {
    time(&rawtime);
    fprintf (outfile, "File updated: %s\n", ctime(&rawtime));

    options.header    = 0;
    options.align     = 0;
    options.fieldSep  = "\n";
    options.html3     = 0;
    options.standard  = 0;
    options.expanded  = 0;
    
    PQprint(outfile, files, &options);
    
    if (fclose(outfile) != 0)
      PURGER_ELOG("process_unwarned_files()", "closing file: %s error: %s", notefile, strerror(errno));
  }
  
  /* Batch-send the e-mails */
  if(moniker[0] != '\0') {
    if (send_mail(uid, filesystem, ldapinfo, mailinfo) != 0) {
      mailerr=1;
      PURGER_ELOG("process_unwarned_files()", "sending email to: %s", uid);
    }
  }
  
  /* update the entries to warned = 1 */
  if ((!mailerr) || (moniker[0] == '\0')) {
    snprintf(update_query, 200, "UPDATE expired_files SET warned = True, added = '%s' WHERE warned = False AND uid = %s;", ins_timenow, uid);
    res = PQexec(conn, update_query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      PURGER_ELOG("process_unwarned_files()", "update warned command failed: %s", PQerrorMessage(conn));
      PQclear(res);
      exit_nicely(conn);
    }
  }
  
  PQclear(files);
  
  return 0;
}

int process_warned_files(PGconn *conn, char *uid, char *filesystem, char *ins_timenow){
  /* counting variables */
  int       i;
  
  /* string variables */
  char      files_query[500];
  
  /* postgres variables */
  PGresult *files;
  PGresult *exceptions;
  int       filename_index;

  /* deletion log */
  FILE     *dlog;

  /* Time struct */
  time_t    rawtime;

  dlog = fopen("/var/log/purger/deletions", "a");
  if (!dlog)
    perror("error opening deletion log");

  time(&rawtime);
  fprintf (dlog, "\n%s\n", ctime(&rawtime));
  
  snprintf(files_query, 1024, "SELECT * FROM exceptions WHERE uid = %s AND expiration > now();", uid);
  exceptions = PQexec(conn, files_query);
  if (PQresultStatus(exceptions) != PGRES_TUPLES_OK) {
    PURGER_ELOG("process_warned_files()", "SELECT * command failed: %s", PQerrorMessage(conn));
    PQclear(exceptions);
    exit_nicely(conn);
  }
  
  if (PQntuples(exceptions) > 0) {
    PURGER_LOG("process_warned_files()", "found exception for uid=%s. Setting all files to unwarned", uid);
    snprintf(files_query, 1024, "UPDATE expired_files SET warned = False WHERE uid=%s;", uid);
    exceptions = PQexec(conn, files_query);
    if (PQresultStatus(exceptions) != PGRES_COMMAND_OK) {
      PURGER_ELOG("process_warned_files()", "update warned command failed: %s", PQerrorMessage(conn));
      PQclear(exceptions);
      PURGER_ELOG("process_warned_files()", "%s", "bailing to protect execptions.\n");
      exit_nicely(conn);
    } 
 
    fclose(dlog);
    return EXIT_SUCCESS;
  }

  snprintf(files_query, 500, "SELECT * FROM expired_files WHERE uid = %s AND filename like '/panfs/%s/vol%%/%%/_%%' AND filename NOT like '/panfs/%s/vol%%/.panfs_store' AND warned = True AND added < CURRENT_TIMESTAMP - INTERVAL '7 days';", uid, filesystem, filesystem);
  files = PQexec(conn, files_query);
  if (PQresultStatus(files) != PGRES_TUPLES_OK) {
    PURGER_ELOG("process_warned_files()", "SELECT * command failed: %s", PQerrorMessage(conn));
    PQclear(files);
    exit_nicely(conn);
  }      
  
  PURGER_LOG("process_warned_files()", "processing %i warned files for uid=%s... ", PQntuples(files), uid);
  
  if (PQntuples(files) == 0) return EXIT_SUCCESS;
  
  if ((PQntuples(files) > MAX_STATS) && (!force)) {
    PURGER_ELOG("main()", "Too many files to process for uid:%s (total=%i), run parallel purge or run with -f to force slow serial purge", uid, PQntuples(files));
    fclose(dlog);
    return EXIT_SUCCESS;
  }
  
  /* Set up indexes */
  filename_index = PQfnumber(files, "filename");
  
  /* ------      BEGIN PROCESSING      ------ */
  /* For each file: */
  
  for (i = 0; i < PQntuples(files); i++)
    /* delete the file here */
    delete_file(PQgetvalue(files, i, filename_index), conn, dlog);
  
  /* PQclear(exceptions); */
  PQclear(files);
  fclose(dlog);
  
  return EXIT_SUCCESS;
}

int send_mail(char *uid, char *filesystem, ldapinfo_t *ldapinfo, mailinfo_t *mailinfo){
  char email[128];
  char moniker[128];
  char notefile[256];
  int ret;
  
  /* grab moniker from uid */
  if (strncmp(uid, "0", 2) == 0) {
    snprintf(moniker, 5, "nfs");
    snprintf(notefile, 256, "/var/log/purger/expired-files-root-%s.txt", filesystem);
  }
  else if (get_moniker( uid, ldapinfo->host, ldapinfo->basem, moniker ) == 1) {
    PURGER_ELOG("send_mail()", "Error getting moniker from ldap host: %s base: %s uid: %s", ldapinfo->host, ldapinfo->basem, uid);
    /* SHOULD WE GET THIS FAR IF NOT?  OTHER GET_MONIKER SHOULD HAVE FAILED */
    return EXIT_FAILURE;
  }
  else
    snprintf(notefile, 256, "/%s/%s/expired-files.txt", filesystem, moniker);
  
  /* grab e-mail from uid */
  if (strncmp(uid, "0", 2) == 0) {
    snprintf(email, 256, mailinfo->defaultto);
  }
  else
    if (get_email( moniker, ldapinfo->host, ldapinfo->base, email ) == 1) {
      PURGER_ELOG("send_mail()", "Error getting email from ldap host: %s base: %s uid: %s\nWill not be able to send mail to %s", ldapinfo->host, ldapinfo->base, uid, moniker);
      /* WHY COULDN'T WE GET AN EMAIL HERE? */
      /* EXIT_SUCCESS so that files still get marked as warned */
      return EXIT_SUCCESS;
    }
  
  /* send e-mail containing list filename */
  ret = sendmail(
		 email,     /* to              */
		 notefile,  /* body            */
		 25,        /* port            */
		 mailinfo   /* mailinfo struct */
		 );
  
  if (ret != 0)
    PURGER_ELOG("send_mail()", "Failed to send mail (code: %i).", ret);
  else
    PURGER_LOG("send_mail()", "Mail successfully sent. (uid %s)", uid);
  
  return ret;
}

void delete_file(char *filename, PGconn *conn, FILE *dlog){
  struct stat st;
  char        files_query[1024];
  PGresult   *files;
  
  /* stat check to verify last time before delete. */
  if (lstat(filename,&st) == -1) {
    PURGER_ELOG("delete_file()", "Failed lstat for %s",filename);
    perror("lstat()");
  } 
  else {
    snprintf(files_query, 1024, "SELECT * FROM expired_files WHERE filename = '%s' AND atime = timestamp without time zone 'epoch' + %ju * interval '1 second' AND mtime = timestamp without time zone 'epoch' + %ju * interval '1 second' AND ctime = timestamp without time zone 'epoch' + %ju * interval '1 second';", filename, st.st_atime, st.st_mtime, st.st_ctime);
    files = PQexec(conn, files_query);
    if (PQresultStatus(files) != PGRES_TUPLES_OK) {
      PURGER_ELOG("delete_file()", "SELECT * command failed: %s\n%s", PQerrorMessage(conn), files_query);
      PQclear(files);
      exit_nicely(conn);
    }
    
    if (PQntuples(files) > 0) {
      fprintf(dlog, "%s %ju %ju %ju\n", filename, st.st_atime, st.st_mtime, st.st_ctime);
      if(remove(filename) != 0) {
	PURGER_ELOG("delete_file()", "Failed remove for %s\n", filename);
	perror("remove()");
      }
    }
  }
    
  snprintf(files_query, 1024, "DELETE FROM expired_files WHERE filename = '%s'", filename);
  files = PQexec(conn, files_query);
  if (PQresultStatus(files) != PGRES_COMMAND_OK) {
    PURGER_ELOG("delete_file()", "DELETE * command failed: %s", PQerrorMessage(conn));
    exit_nicely(conn);      
  }
  
  PQclear(files);
}
