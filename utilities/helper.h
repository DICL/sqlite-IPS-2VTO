#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/syscall.h>

#include "cvector.h"
#include "../sources/sqlite3.h"

// DICL: for statistic utilities
#ifdef GET_STATISTIC
  int fsync_cnt = 0;
  int write_cnt = 0;
  int page1_cnt = 0;
  _Atomic unsigned long long total_write_time = 0;
  _Atomic unsigned long long total_fsync_time = 0;
  _Atomic unsigned long long io_time      = 0;
  _Atomic unsigned long long compute_time = 0;
  _Atomic unsigned long long sql_prepare  = 0;
  _Atomic unsigned long long rollback_time = 0;
  _Atomic unsigned long long wait_time    = 0;
  _Atomic unsigned long long lock_time    = 0;
  _Atomic long commit_time = 0;
  _Atomic int replay_count  = 0;
  _Atomic int leader_wait_time = 0;
  struct rusage usage_start, usage_end;
#endif

// DICL: required global vars for DHL
int util=0;
int page_size=4096;
int nData=10000;
int record_size=100;

#define NO_KEYS       10000
#define HALT          ({*(int*)0 = 0;})
#define TID           (syscall(__NR_gettid))
#define RANDOM_AGE    (rand() % NO_KEYS + 1)
#define VFS_FLAGS     (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_SHAREDCACHE)
#define PRIVATE_FLAGS (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_PRIVATECACHE)

#ifndef NO_THREADS
# define NO_THREADS    4
#endif

#define IS_COMMAND(query, command) (strncmp((query), (command), strlen(command)) == 0)

#define DEBUG_LOADING(db, TABLE) ({                                                   \
  char sql_query[200];                                                                \
  sprintf(sql_query, "SELECT * FROM %s", TABLE);                                      \
  rc = sqlite3_exec(db, sql_query, recordCallback, NULL, NULL);                       \
  if (rc) {                                                                           \
    printf("Can't execute query, err: %s\n", sqlite3_errmsg(db));                     \
    return rc;                                                                        \
  }                                                                                   \
})

#define DEBUG_LOADING_COUNT(db, TABLE) ({                                             \
  char sql_query[200];                                                                \
  sprintf(sql_query, "SELECT COUNT(*) FROM %s", TABLE);                               \
  rc = sqlite3_exec(db, sql_query, recordCallback, NULL, NULL);                       \
  if (rc) {                                                                           \
    printf("Can't execute query, err: %s\n", sqlite3_errmsg(db));                     \
    return rc;                                                                        \
  }                                                                                   \
})

double timespec_diff(struct timespec start, struct timespec end) {
  return (1000 * (end.tv_sec - start.tv_sec) + (double)(end.tv_nsec - start.tv_nsec) / 1000000);
}

int recordCallback(void *nil, int cnt, char **data, char **colNames) {
  char record[1200], buf[200];
#ifdef DHL_DEBUG_THREADID
  snprintf(buf, sizeof buf, "%ld: Record: [", syscall(__NR_gettid));
#else
  snprintf(buf, sizeof buf, "Record: [");
#endif
  strcpy(record, buf);
  for(int idx = 0; idx < cnt; idx ++){
    char *end = (idx != cnt - 1) ? ", " :"]";
    snprintf(buf, sizeof buf, "%s: %s%s", colNames[idx], data[idx] ? data[idx] : "NULL", end);
    strcat(record, buf);
  }
  printf("%s\n", record);
  return 0;
}

int createTestDatabase(sqlite3 **db) {
  char *err_msg;

  // Create a new database file from scratch
  remove("test.db");
  int rc = sqlite3_open_v2("test.db", db, VFS_FLAGS, NULL);
  if (rc) {
    printf("Can't open database %s, err: %s\n", "test.db", sqlite3_errmsg(*db));
    return rc;
  }

  sqlite3_exec(*db, "PRAGMA journal_mode=OFF", NULL, NULL, NULL);

  // SQLite create database
  rc = sqlite3_exec(*db,
    "CREATE TABLE IF NOT EXISTS COMPANY("  \
    "id INTEGER PRIMARY KEY NOT NULL," \
    "age            INT     NOT NULL);",
    NULL, 0, &err_msg);

  if (rc) {
    printf("Can't create database, err: %s\n", err_msg);
    return rc;
  }

#ifdef GET_STATISTIC
  getrusage(RUSAGE_SELF, &usage_start);
#endif

  return rc;
}

#ifdef GET_STATISTIC

void resetStatistic() {
  fsync_cnt = 0;
  write_cnt = 0;
  page1_cnt = 0;
  total_write_time = 0;
  total_fsync_time = 0;
  leader_wait_time = 0;
  io_time       = 0;
  compute_time  = 0;
  sql_prepare   = 0;
  replay_count  = 0;
  rollback_time = 0;
  wait_time     = 0;
  commit_time   = 0;
  lock_time     = 0;
  getrusage(RUSAGE_SELF, &usage_start);
}

void finishScript(unsigned long long total_exec_time, int num_threads) {
#ifdef GET_STATISTIC
  getrusage(RUSAGE_SELF, &usage_end);
  unsigned long long utime = 1000000LLU * (usage_end.ru_utime.tv_sec - usage_start.ru_utime.tv_sec) + \
                   usage_end.ru_utime.tv_usec - usage_start.ru_utime.tv_usec;
  unsigned long long stime = 1000000LLU * (usage_end.ru_stime.tv_sec - usage_start.ru_stime.tv_sec) + \
                   usage_end.ru_stime.tv_usec - usage_start.ru_stime.tv_usec;

  // DICL: Convert to microseconds
  total_fsync_time /= 1000;
  total_write_time /= 1000;
  io_time /= 1000;
  sql_prepare /= 1000;
  rollback_time /= 1000;
  wait_time /= 1000;
  lock_time /= 1000;
  compute_time = total_exec_time * num_threads - total_write_time - total_fsync_time - rollback_time - wait_time - lock_time;

#ifdef HUMAN_READABLE_STATISTIC
  int context_sw = usage_end.ru_nvcsw - usage_start.ru_nvcsw;
  int force_sw = usage_end.ru_nivcsw - usage_start.ru_nivcsw;

  printf("user mode %ld - kernel mode %ld - read cnt %ld - write cnt %ld - context switch %d - force ctx switch %d\n",
    utime, stime, usage_end.ru_inblock - usage_start.ru_inblock, usage_end.ru_oublock - usage_start.ru_oublock, context_sw, force_sw);

  printf("leader wait amount of time: %.2d ms\n", leader_wait_time / 1000);
  printf("number of times page 1 was written: %d\n", page1_cnt);
  printf("total_fsync_time %llu - fsync cnt %d\n", total_fsync_time, fsync_cnt);
  printf("total_write_time %llu - write cnt %d\n", total_write_time, write_cnt);
  printf("number of replayed txns: \t%d\n", replay_count);
  printf("sql preparation time: \t\t%lu\n", sql_prepare);
  printf("computation time: \t\t%llu\n", compute_time);
  printf("io time: \t\t\t%llu\n", io_time);
  printf("commit time: \t\t\t%ld\n", commit_time);
  printf("rollback time: \t\t\t%ld\n", rollback_time);
#else
  printf("Computation,Wait,Lock,Fsync,Write,Rollback,Replay\n");
  printf("%llu,%llu,%llu,%llu,%llu,%llu,%d\n",
         compute_time, wait_time, lock_time, total_fsync_time, total_write_time, rollback_time, replay_count);
#endif

#endif
}

#else

#define resetStatistic()
#define finishScript()

#endif