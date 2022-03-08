#include <stdio.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <malloc.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <locale.h>
#include <pthread.h>
#include <stdlib.h>

#include "../sources/sqlite3.h"

#define INPUTRANGE 100000000LL

extern int Aborted;
extern int transFlag;

// DICL: for txn Manager initialization
int num_threads = 1;

int walsize = 0;
struct timespec Rs, Re;
char * db_name = "/cache/test.db";
long long *input=NULL;
long long *output=NULL;
//int *output;
int cnt=0;
int util = 0;
int num_cycle=0;
int page_size = 4;
long long total_write_time=0;
long long total_fsync_time=0;
int write_cnt=0;
int fsync_cnt=0;
long long avg_write_time = 0;
long long avg_fsync_time = 0;
int MAX_SH_SIZE = 0;
int correctness = 0;
int store_type = 0;
int Env = 0;
int op_type = 0;
int mycur = 1;

struct timespec t3,t4;

extern char* optarg;
  //Variable initialization
  char buf[1000];
  char *zErr = NULL;
  int rc = 0;
  char *sql = NULL;
  sqlite3 *db = NULL;
  int cret = 0;
  int nData = 1000;
  int numOp = 1;
  int seed = 0;
  int nThread = 0;
  int output_type = 0;
//  int num_repeat = 1;
  int journal_mode = 0;
  int recovery_op = 0;
  int record_size = 80;
  int select_opt = 0;
  int rm_opt = 0;
  char * record = NULL;
  char * update_record = NULL;
  int callback_cnt = 0;
  long long recovery_time = 0;

int compare(const void * a, const void * b){
  if ( *(long long*)a <  *(long long*)b ) return -1;
  if ( *(long long*)a >  *(long long*)b ) return 1;
  return 0;
}

static int callback(void *data, int argc, char **argv, char **azColName){
  static int i = 0;
  if(op_type == 2){
    if(atoll(argv[0]) == output[i]){ // correct record
      printf("%d key: %lld value: %s\n", i, atoll(argv[0]),argv[1]);
    } else {
      printf("%d key: %lld value: %s (incorrect! expected record key is %lld.)\n", i, atoll(argv[0]), argv[1], output[i]);
      correctness = 1;
    }
    i++;
    callback_cnt++;
    return 0;
  }
  if(atoll(argv[0]) == output[i]){ // correct record
    printf("%d key: %lld\n", i, atoll(argv[0]));
  } else {
    printf("%d key: %lld (incorrect! expected record key is %lld.)\n", i, atoll(argv[0]), output[i]);
    correctness = 1;
  }
  i++;
  callback_cnt++;
  return 0;
}
int main(int argc, char **argv){
  if(argc == 1){ // go to manual
    goto help;
  }
  while((cret=getopt(argc,argv,"n:o:s:c:p:r:j:d:e:R:P:t:T:S:h:w:x"))!=EOF){
    switch(cret){
      case 'n':
        nData = atoi(optarg);
        break;
      case 'o':
        numOp = atoi(optarg);

        break;
      case 'p':
        output_type = atoi(optarg);

        break;
      case 'j':
        journal_mode = atoi(optarg);

        break;
      case 'r':
        recovery_op = atoi(optarg);

        break;
      case 'R':
        record_size = atoi(optarg);

        break;
      case 'P':
        page_size = atoi(optarg);

        break;
      case 'S':
        select_opt = atoi(optarg);

        break;
      case 'x':
        rm_opt = atoi(optarg);

        break;
      case 'h':
help:
        printf("-n val : (WARM-UP) val number of records(default = 1000)\n");
        printf("-o val : val operations / 1 Txn(default = 1)\n");
        printf("-p val : set the output format.\n");
        printf("         0 : print all the detail.(default)\n");
        printf("         1 : print total execution time only.\n");
        //        printf("-r val : repeat same work val times.(default = 1)\n");
        printf("-j val : set journal mode.\n");
        printf("         0 : OFF(default)\n");
        printf("         1 : WAL\n");
        printf("         2 : MEMORY\n");
        printf("         3 : DELETE\n");
        printf("         4 : waldio\n");
        printf("         5 : shadow\n");
        printf("         6 : mvbt\n"); // TODO:insert LARGEST before task.
        printf("-r val : set recovery step.\n");
        printf("         0 : create aborted db file \n");
        printf("         1 : do recovery on the db file \n");
        printf("-R val : set record size to val byte. multiple of 8 is recommended.(default = 80)\n");
        printf("-P val : set page size to val * 1024(1k) byte.(default = 4)\n"); // TODO: in sqlite file, use this value for page size, not SQLITE_DEFAULT_PAGE_SIZE.
        printf("-S val : setting select and correctness check\n");
        printf("         0 : turn off select option(default)\n");
        printf("         1 : turn on select option\n");
        printf("         2 : turn on select option with re-open db file.(j.db only)\n");
        printf("-x val : setting whether removing j.db file or not after execution\n");
        printf("         0 : remove j.db file(default)\n");
        printf("         1 : do not remove j.db file\n");
        printf("-h val : help\n");
        return 0;
    }
  }
  //Page size computing
  page_size *= 1024;

  //seed setting.
  srand(seed);
  //input setting : random value
  input = (long long *)malloc(sizeof(long long)*(nData+numOp+10));
  output = (long long *)malloc(sizeof(long long)*(nData+numOp+10));
  int *hash = malloc(sizeof(int)*INPUTRANGE);
  long long j;
  int i;
  for( i = 0; i < INPUTRANGE; i++){
    hash[i] = 0;
  }
  long long key;
  for(j = 0; j<nData+numOp+10; j++){
    while(1){
      key = rand()%INPUTRANGE;
      if(hash[key] == 1) continue;
      hash[key] = 1;
      input[j] = key;
      output[j] = key;
      break;
    }
  }
  //For correction check.
  long long * for_free;
  output[nData] = INPUTRANGE;
  output[nData+1] = input[nData+numOp];
  qsort(output,nData+2,sizeof(long long),compare);
  for_free = output;
  //Setting record regarding record size.
  record = (char *)malloc(record_size+1);
  memset(record,'a',record_size);
  memset(record+record_size,'\0',1);
  sql = "insert into episodes values ('%lld', '%s')";
  if(recovery_op == 0){
    transFlag = 1;
    rc = sqlite3_open(db_name, &db);
    if(rc){
      fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
      sqlite3_close(db);
      return 0;
    }
    //setting journal mode
    if(journal_mode == 0 || journal_mode == 6){
      rc = sqlite3_exec(db, "PRAGMA journal_mode=OFF ", NULL, NULL, &zErr);
    }
    else if(journal_mode == 1){
      rc = sqlite3_exec(db, "PRAGMA journal_mode=WAL ", NULL, NULL, &zErr);
    }
    else if(journal_mode == 2){
      rc = sqlite3_exec(db, "PRAGMA journal_mode=MEMORY ", NULL, NULL, &zErr);
    }
    else if(journal_mode == 3){
      rc = sqlite3_exec(db, "PRAGMA journal_mode=DELETE ", NULL, NULL, &zErr);
    }
    else if(journal_mode == 4){
      if(op_type == 0){
        rc = sqlite3_exec(db, "PRAGMA journal_mode=waldio ", NULL, NULL, &zErr);
      }
      else {
        rc = sqlite3_exec(db, "PRAGMA journal_mode=OFF ", NULL, NULL, &zErr);
      }
    }
    else if(journal_mode == 5){
      rc = sqlite3_exec(db, "PRAGMA journal_mode=shadow ", NULL, NULL, &zErr);
    }
    else if(rc != SQLITE_OK){
      if(zErr != NULL){
        fprintf(stderr, "SQL err: %s\n", zErr);
        sqlite3_free(zErr);
      }
    }
    else{
      printf("ERROR(journal_mode) : Please read help carefully.\n");
      return 0;
    }
    //setting page size.
    sql = "PRAGMA page_size=%d;";
    sprintf(buf, sql, page_size);
    rc=sqlite3_exec(db,buf, NULL, NULL, &zErr);
    if(rc != SQLITE_OK){
      if(zErr != NULL){
        fprintf(stderr, "SQL err: %s\n", zErr);
        sqlite3_free(zErr);
      }
    }
    //creating new table 'episodes'
    sql = "create table episodes(id integer primary key, name text)";
    rc = sqlite3_exec(db, sql, NULL, NULL, &zErr);
    if(rc != SQLITE_OK){
      if(zErr != NULL){
        fprintf(stderr, "SQL err: %s\n", zErr);
        sqlite3_free(zErr);
      }
    }

    //For LSMVBT(and fairness)
    sql = "insert into episodes values ('%lld', '%s')";
    sprintf(buf, sql, INPUTRANGE,record);
    rc = sqlite3_exec(db, buf, NULL, NULL, &zErr);

    //Warm-up nData records
    int numOp_ = 8;
    int num_txn = nData / numOp_;
    int remainder = nData - (num_txn * numOp_);
    int k;
    int iter = 0;
    //printf("insertion BEGIN\n");
    for(j = 0; j < num_txn+1; j++){
      rc = sqlite3_exec(db, "BEGIN", NULL, NULL, &zErr);
      if(rc != SQLITE_OK){
        if(zErr != NULL){
          fprintf(stderr, "SQL err: %s [%d]\n", zErr, __LINE__);
          sqlite3_free(zErr);
        }
      }

      for(k = 0; (j < num_txn && k < numOp_) || (j == (num_txn) && k < remainder); k++){

        //printf("input[%d] : %llu\n", iter, input[iter]);
        sprintf(buf, sql, input[iter],record);
        rc = sqlite3_exec(db, buf, NULL, NULL, &zErr);
        if(rc != SQLITE_OK){
          if(zErr != NULL){
            fprintf(stderr, "SQL err: %s [%d]\n", zErr, __LINE__);
            sqlite3_free(zErr);
          }
        }
        iter++;
      }
      rc = sqlite3_exec(db, "COMMIT", NULL, NULL, &zErr);
      if(rc != SQLITE_OK){
        if(zErr != NULL){
          fprintf(stderr, "SQL err: %s [%d]\n", zErr, __LINE__);
          //printf("key : %ld j: %d\n",key, j);
          sqlite3_free(zErr);
        }
      }
    }


    //printf("Aborted Txn Start\n");
    //Aborted Txn
    rc = sqlite3_exec(db, "BEGIN", NULL, NULL, &zErr);
    if(rc != SQLITE_OK){
      if(zErr != NULL){
        fprintf(stderr, "SQL err: %s [%d]\n", zErr, __LINE__);
        sqlite3_free(zErr);
      }
    }
    Aborted = 1;

    for(k = 0; k < numOp; k++){

      //printf("input[%d] : %llu\n", iter, input[iter]);
      sprintf(buf, sql, input[iter],record);
      rc = sqlite3_exec(db, buf, NULL, NULL, &zErr);
      if(rc != SQLITE_OK){
        if(zErr != NULL){
          fprintf(stderr, "SQL err: %s [%d]\n", zErr, __LINE__);
          sqlite3_free(zErr);
        }
      }
      iter++;
    }
    //Some pages are not written.
    if(journal_mode == 0 || journal_mode == 5){
    rc = sqlite3_exec(db, "COMMIT", NULL, NULL, &zErr);
    if(rc != SQLITE_OK){
      if(zErr != NULL){
        fprintf(stderr, "SQL err: %s [%d]\n", zErr, __LINE__);
        //printf("key : %ld j: %d\n",key, j);
        sqlite3_free(zErr);
      }
    }
    }
    //printf("Aborted Txn End\n");
    Aborted = 0;
    return 0;
  } else if(recovery_op > 0){
    Aborted = 0;
    transFlag = 0;
    int k;
    int iter = nData + numOp;
    //printf("Reopen!\n");
    db = NULL;
    //Reopen the aborted db
    rc = sqlite3_open(db_name, &db);
    if(rc){
      fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
      sqlite3_close(db);
      return 0;
    }
    //setting journal mode
    if(journal_mode == 0 || journal_mode == 6){
      rc = sqlite3_exec(db, "PRAGMA journal_mode=OFF ", NULL, NULL, &zErr);
    }
    else if(journal_mode == 1){
      rc = sqlite3_exec(db, "PRAGMA journal_mode=WAL ", NULL, NULL, &zErr);
      sqlite3_exec(db, "PRAGMA wal_autocheckpoint=100;", NULL, NULL, &zErr);
      sqlite3_exec(db, "PRAGMA journal_size_limit=524288;", NULL, NULL, &zErr);
    }
    else if(journal_mode == 2){
      rc = sqlite3_exec(db, "PRAGMA journal_mode=MEMORY ", NULL, NULL, &zErr);
    }
    else if(journal_mode == 3){
      rc = sqlite3_exec(db, "PRAGMA journal_mode=DELETE ", NULL, NULL, &zErr);
    }
    else if(journal_mode == 4){
      if(op_type == 0){
        rc = sqlite3_exec(db, "PRAGMA journal_mode=waldio ", NULL, NULL, &zErr);
      }
      else {
        rc = sqlite3_exec(db, "PRAGMA journal_mode=OFF ", NULL, NULL, &zErr);
      }
    }
    else if(journal_mode == 5){
      rc = sqlite3_exec(db, "PRAGMA journal_mode=shadow", NULL, NULL, &zErr);
    }
    else if(rc != SQLITE_OK){
      if(zErr != NULL){
        fprintf(stderr, "SQL err: %s\n", zErr);
        sqlite3_free(zErr);
      }
    }
    else{
      printf("ERROR(journal_mode) : Please read help carefully.\n");
      return 0;
    }
    //TODO: Recovery db file
  sync();
  system("echo 3 > /proc/sys/vm/drop_caches");
   if(journal_mode != 1){
      //DHL, IPS and LSMVBT
      for(k = 0; k < 1 ; k++){
        rc = sqlite3_exec(db, "BEGIN", NULL, NULL, &zErr);
        if(rc != SQLITE_OK){
          if(zErr != NULL){
            fprintf(stderr, "SQL err: %s [%d]\n", zErr, __LINE__);
            sqlite3_free(zErr);
          }
        }
        //printf("input[%d] : %llu\n", iter, input[iter]);
        sprintf(buf, sql, input[iter],record);
        rc = sqlite3_exec(db, buf, NULL, NULL, &zErr);
        if(rc != SQLITE_OK){
          if(zErr != NULL){
            fprintf(stderr, "SQL err: %s [%d] - rc %d\n", zErr, __LINE__, rc);
            sqlite3_free(zErr);
          }
        }
        iter++;
        rc = sqlite3_exec(db, "COMMIT", NULL, NULL, &zErr);
        if(rc != SQLITE_OK){
          if(zErr != NULL){
            fprintf(stderr, "SQL err: %s [%d]\n", zErr, __LINE__);
            //printf("key : %ld j: %d\n",key, j);
            sqlite3_free(zErr);
          }
        }
      }
      recovery_time = 1000000000LLU*(Re.tv_sec - Rs.tv_sec);
      recovery_time += (Re.tv_nsec - Rs.tv_nsec);
    } else {
      //TODO: Find the function and the case of the recovery of Journaling and WAL.
      sqlite3_wal_checkpoint(db, NULL);
      recovery_time = 1000000000LLU*(Re.tv_sec - Rs.tv_sec);
      recovery_time += (Re.tv_nsec - Rs.tv_nsec);
      //printf("WalSize: %d\n", walsize);
    }
    if(select_opt){
      printf("selection BEGIN\n");
      rc = sqlite3_exec(db, "SELECT * FROM episodes;", callback, NULL, &zErr);
      if(rc != SQLITE_OK){
        if(zErr != NULL){
          fprintf(stderr, "SQL err: %s [%d]\n", zErr, __LINE__);
          sqlite3_free(zErr);
        }
      }
      printf("selection END\n");
      //} else if(correctness == 0){ // Correct result
      //  printf("CORRECT : record insertion completes without error.\n");
      //} else {
      //  printf("ERROR : there are some incorrect point! please check the result.\n");
      //}

  }
  sqlite3_close(db);
  if(output_type == 0){
    printf("Recovery Time(ns): %20llu\n" , recovery_time);
  } else {
    printf("%20llu \n" , recovery_time);
  }

  system("rm -f oh.db*");
  unlink("oh.db");
}
return 0;
}
