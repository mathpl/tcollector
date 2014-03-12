#!/usr/bin/python
# This file is part of tcollector.
# Copyright (C) 2011  The tcollector Authors.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.  This program is distributed in the hope that it
# will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
# General Public License for more details.  You should have received a copy
# of the GNU Lesser General Public License along with this program.  If not,
# see <http://www.gnu.org/licenses/>.
"""Collector for MySQL."""

import errno
import os
import re
import socket
import sys
import time

try:
  import MySQLdb
except ImportError:
  MySQLdb = None  # This is handled gracefully in main()

from collectors.etc import mysqlconf
from collectors.lib import utils

COLLECTION_INTERVAL = 15  # seconds
CONNECT_TIMEOUT = 2  # seconds
# How frequently we try to find new databases.
DB_REFRESH_INTERVAL = 60  # seconds
# Usual locations where to find the default socket file.
DEFAULT_SOCKFILES = set([
  "/tmp/mysql.sock",                  # MySQL's own default.
  "/var/lib/mysql/mysql.sock",        # RH-type / RPM systems.
  "/var/run/mysqld/mysqld.sock",      # Debian-type systems.
])
# Directories under which to search additional socket files.
SEARCH_DIRS = [
  "/var/lib/mysql",
]

def err(msg):
  print >>sys.stderr, msg

class DB(object):
  """Represents a MySQL server (as we can monitor more than 1 MySQL)."""

  def __init__(self, sockfile, dbname, db, cursor, version):
    """Constructor.

    Args:
      sockfile: Path to the socket file.
      dbname: Name of the database for that socket file.
      db: A MySQLdb connection opened to that socket file.
      cursor: A cursor acquired from that connection.
      version: What version is this MySQL running (from `SELECT VERSION()').
    """
    self.sockfile = sockfile
    self.dbname = dbname
    self.db = db
    self.cursor = cursor
    self.version = version
    self.master = None
    self.slave_bytes_executed = None
    self.relay_bytes_relayed = None

    version = version.split(".")
    try:
      self.major = int(version[0])
      self.medium = int(version[1])
    except (ValueError, IndexError), e:
      self.major = self.medium = 0

  def __str__(self):
    return "DB(%r, %r, version=%r)" % (self.sockfile, self.dbname,
                                       self.version)

  def __repr__(self):
    return self.__str__()

  def isShowGlobalStatusSafe(self):
    """Returns whether or not SHOW GLOBAL STATUS is safe to run."""
    # We can't run SHOW GLOBAL STATUS on versions prior to 5.1 because it
    # locks the entire database for too long and severely impacts traffic.
    return self.major > 5 or (self.major == 5 and self.medium >= 1)

  def query(self, sql):
    """Executes the given SQL statement and returns a sequence of rows."""
    assert self.cursor, "%s already closed?" % (self,)
    try:
      self.cursor.execute(sql)
    except MySQLdb.OperationalError, (errcode, msg):
      if errcode != 2006:  # "MySQL server has gone away"
        raise
      self._reconnect()
    return self.cursor.fetchall()

  def close(self):
    """Closes the connection to this MySQL server."""
    if self.cursor:
      self.cursor.close()
      self.cursor = None
    if self.db:
      self.db.close()
      self.db = None

  def _reconnect(self):
    """Reconnects to this MySQL server."""
    self.close()
    self.db = mysql_connect(self.sockfile)
    self.cursor = self.db.cursor()


def mysql_connect(sockfile):
  """Connects to the MySQL server using the specified socket file."""
  user, passwd = mysqlconf.get_user_password(sockfile)
  return MySQLdb.connect(unix_socket=sockfile,
                         connect_timeout=CONNECT_TIMEOUT,
                         user=user, passwd=passwd)


def todict(db, row):
  """Transforms a row (returned by DB.query) into a dict keyed by column names.

  Args:
    db: The DB instance from which this row was obtained.
    row: A row as returned by DB.query
  """
  d = {}
  for i, field in enumerate(db.cursor.description):
    column = field[0].lower()  # Lower-case to normalize field names.
    d[column] = row[i]
  return d

def get_dbname(sockfile):
  """Returns the name of the DB based on the path to the socket file."""
  if sockfile in DEFAULT_SOCKFILES:
    return "default"
  m = re.search("/mysql-(.+)/[^.]+\.sock$", sockfile)
  if not m:
    err("error: couldn't guess the name of the DB for " + sockfile)
    return None
  return m.group(1)


def find_sockfiles():
  """Returns a list of paths to socket files to monitor."""
  paths = []
  # Look for socket files.
  for dir in SEARCH_DIRS:
    if not os.path.isdir(dir):
      continue
    for name in os.listdir(dir):
      subdir = os.path.join(dir, name)
      if not os.path.isdir(subdir):
        continue
      for subname in os.listdir(subdir):
        path = os.path.join(subdir, subname)
        if utils.is_sockfile(path):
          paths.append(path)
          break  # We only expect 1 socket file per DB, so get out.
  # Try the default locations.
  for sockfile in DEFAULT_SOCKFILES:
    if not utils.is_sockfile(sockfile):
      continue
    paths.append(sockfile)
  return paths


def find_databases(dbs=None):
  """Returns a map of dbname (string) to DB instances to monitor.

  Args:
    dbs: A map of dbname (string) to DB instances already monitored.
      This map will be modified in place if it's not None.
  """
  sockfiles = find_sockfiles()
  if dbs is None:
    dbs = {}
  for sockfile in sockfiles:
    dbname = get_dbname(sockfile)
    if dbname in dbs:
      continue
    if not dbname:
      continue
    try:
      db = mysql_connect(sockfile)
      cursor = db.cursor()
      cursor.execute("SELECT VERSION()")
    except (EnvironmentError, EOFError, RuntimeError, socket.error,
            MySQLdb.MySQLError), e:
      err("Couldn't connect to %s: %s" % (sockfile, e))
      continue
    version = cursor.fetchone()[0]
    dbs[dbname] = DB(sockfile, dbname, db, cursor, version)
  return dbs


def now():
  return int(time.time())


def isyes(s):
  if s.lower() == "yes":
    return 1
  return 0


def collectInnodbStatus(db):
  """Collects and prints InnoDB stats about the given DB instance."""
  ts = now()
  def printmetric(metric, value, tags=""):
    print "mysql.innodb.%s %d %s schema=%s%s" % (metric, ts, value, db.dbname, tags)

  innodb_status = db.query("SHOW ENGINE INNODB STATUS")[0][2]
  m = re.search("^(\d{6}\s+\d{1,2}:\d\d:\d\d) INNODB MONITOR OUTPUT$",
                innodb_status, re.M)
  if m:  # If we have it, try to use InnoDB's own timestamp.
    ts = int(time.mktime(time.strptime(m.group(1), "%y%m%d %H:%M:%S")))

  line = None
  def match(regexp):
    return re.match(regexp, line)

  counters_metrics = ["tables_in_use",
                      "tables_locked",
                      "sem_waits",
                      "sem_wait_time_ms",
                      "current_transactions",
                      "active_transactions",
                      "lock_wait_secs",
                      "lock_structs",
                      "locked_transactions"]

  counters= {}
  for key in counters_metrics:
    counters[key] = 0

  for line in innodb_status.split("\n"):
    try:
      # SEMAPHORES
      m = match("OS WAIT ARRAY INFO: reservation count (\d+), signal count (\d+)")
      if m:
        printmetric("oswait_array.reservation_count", m.group(1))
        printmetric("oswait_array.signal_count", m.group(2))
        continue
      m = match("Mutex spin waits (\d+), rounds (\d+), OS waits (\d+)")
      if m:
        printmetric("locks.spin_waits", m.group(1), " type=mutex")
        printmetric("locks.rounds", m.group(2), " type=mutex")
        printmetric("locks.os_waits", m.group(3), " type=mutex")
        continue
      m = match("RW-shared spins (\d+), OS waits (\d+);"
                " RW-excl spins (\d+), OS waits (\d+)")
      if m:
        printmetric("locks.spin_waits", m.group(1), " type=rw-shared")
        printmetric("locks.os_waits", m.group(2), " type=rw-shared")
        printmetric("locks.spin_waits", m.group(3), " type=rw-exclusive")
        printmetric("locks.os_waits", m.group(4), " type=rw-exclusive")
        continue

      # INSERT BUFFER AND ADAPTIVE HASH INDEX
      # TODO(tsuna): According to the code in ibuf0ibuf.c, this line and
      # the following one can appear multiple times.  I've never seen this.
      # If that happens, we need to aggregate the values here instead of
      # printing them directly.
      #m = match("Ibuf: size (\d+), free list len (\d+), seg size (\d+),")
      m = match("Ibuf: size (\d+), free list len (\d+), seg size (\d+)")
      if m:
        printmetric("ibuf.size", m.group(1))
        printmetric("ibuf.free_list_len", m.group(2))
        printmetric("ibuf.seg_size", m.group(3))
        m = match(".* (\d+) merges")
        if m:
          printmetric("ibuf.merges", m.group(1))

        continue
      m = match(" insert (\d+), delete mark (\d+), delete (\d+)")
      if m:
        printmetric("ibuf.inserts", m.group(1))
        printmetric("ibuf.merged", long(m.group(2)) + long(m.group(3)))
        continue
      # ROW OPERATIONS
      m = match("(\d+) queries inside InnoDB, (\d+) queries in queue")
      if m:
        printmetric("queries_inside", m.group(1))
        printmetric("queries_queued", m.group(2))
        continue
      m = match("(\d+) read views open inside InnoDB")
      if m:
        printmetric("opened_read_views", m.group(1))
        continue
      # Transaction history list
      m = match("History list length (\d+)")
      if m:
        printmetric("history_list_length", m.group(1))
        continue

      # FILE I/O
      m = match(" ibuf aio reads: (\d+), log i/o's: (\d+), sync i/o's: (\d+)")
      if m:
        printmetric("pending_ibuf_aio_reads", m.group(1))
        printmetric("pending_aio_log_ios", m.group(2))
        printmetric("pending_aio_sync_ios", m.group(3))
        continue
      m = match("Pending flushes \(fsync\) log: (\d+); buffer pool: (\d+)")
      if m:
        # pending_log_flushes == Innodb_os_log_pending_fsyncs from show status
        #printmetric("pending_log_flushes", m.group(1))
        printmetric("pending_buffer_pool_flushes", m.group(2))
        continue


      # Log
      m = match("Log sequence number\s+(\d+)")
      if m:
        #Deal with bigint
        n = match("Log sequence number\s+(\d+)\s+(\d+)")
        if n:
          log_seq = long(n.group(1) + n.group(2))
        else:
          log_seq = long(m.group(1))
        printmetric("log_bytes_written", log_seq)
        continue
      m = match("Log flushed up to\s+(\d+)")
      if m:
        #Deal with bigint
        n = match("Log flushed up to\s+(\d+)\s+(\d+)")
        if n:
          log_flush_up_to = long(n.group(1) + n.group(2))
        else:
          log_flush_up_to = long(m.group(1))
        printmetric("log_bytes_flushed", log_flush_up_to)
        printmetric("unflushed_log", log_seq - log_flush_up_to)
        continue
      m = match("Last checkpoint at\s+(\d+)")
      if m:
        #Deal with bigint
        n = match("Last checkpoint at\s+(\d+)\s+(\d+)")
        if n:
          last_checkpoint = long(n.group(1) + n.group(2))
        else:
          last_checkpoint = long(m.group(1))
        printmetric("last_checkpoint", log_flush_up_to)
        printmetric("uncheckpointed_bytes", log_seq - last_checkpoint)
        continue
      m = match("(\d+) pending log writes, (\d+) pending chkp writes")
      if m:
        printmetric("pending_log_writes", m.group(1))
        printmetric("pending_checkpoint_writes", m.group(2))
        continue
      m = match("(\d+) log i/o's done, ")
      if m:
        printmetric("log_writes", m.group(1))
        continue

      # Buffer pool and memory
      m = match("Dictionary memory allocated\s+(\d+)")
      if m:
        printmetric("dictionary_memory", m.group(1))
      m = match("Free buffers\s+(\d+)")
      if m:
        printmetric("buffer_pool.free_buffers", m.group(1))
        continue
      m = match("Database pages\s+(\d+)")
      if m:
        printmetric("buffer_pool.db_pages", m.group(1))
        continue
      m = match("Old database pages\s+(\d+)")
      if m:
        printmetric("buffer_pool.old_db_pages", m.group(1))
        continue
      m = match("Modified db pages\s+(\d+)")
      if m:
        printmetric("buffer_pool.modified_db_pages", m.group(1))
        continue
      m = match("Total memory allocated (\d+); in additional pool allocated (\d+)")
      if m:
        printmetric("total_mem_alloc", m.group(1))
        printmetric("additional_pool_alloc", m.group(2))
        continue
      m = match("Buffer pool hit rate (\d+) / 1000, young-making rate (\d+) / 1000 not (\d+) / 1000")
      if m:
        printmetric("buffer_pool.hit_rate", m.group(1))
        printmetric("buffer_pool.young_making_rate", m.group(2))
        printmetric("buffer_pool.not_rate", m.group(3))
        continue

      #Tables in use/locked
      m = match("mysql tables in use (\d+), locked (\d+)")
      if m:
        counters["tables_in_use"] = int(m.group(1)) + counters["tables_in_use"]
        counters["tables_locked"] = int(m.group(2)) + counters["tables_locked"]
        continue

      #Semaphore wait count and total
      m = match(".*for (\d+\.\d+) seconds the semaphore:")
      if m:
        counters["sem_waits"] = counters["sem_waits"] + 1
        counters["sem_wait_time_ms"] = int(float(m.group(1))*1000) + counters["sem_wait_time_ms"]
        continue

      #Transactions
      m = match("Trx id counter (\S+)")
      if m:
        #Deal with bigint
        n = match("Trx id counter (\S+) (\S+)")
        if n:
          trx_count = long(n.group(1) + n.group(2), 16)
        else:
          trx_count = long(m.group(1), 16)
        printmetric("transactions", trx_count)
        continue
      m = match("Purge done for trx's n:o < (\S+)")
      if m:
        #Deal with bigint
        n = match("Purge done for trx's n:o < (\S+) (\S+) undo")
        if n:
          unpurged_trx_count = long(n.group(1) + n.group(2), 16)
        else:
          unpurged_trx_count = long(m.group(1), 16)
        unpurged_trx_diff = trx_count - unpurged_trx_count
        printmetric("unpurged_transactions", unpurged_trx_diff)
        continue
      m = match("---TRANSACTION")
      if m:
        counters["current_transactions"] = counters["current_transactions"] + 1
        m = match(".*ACTIVE")
        if m:
          counters["active_transactions"] = counters["active_transactions"] + 1
        continue
      m = match("------- TRX HAS BEEN WAITING (\d+) SEC")
      if m:
        counters["lock_wait_secs"] = int(m.group(1)) + counters["lock_wait_secs"]
        continue
      m = match("(\d+) read views open inside InnoDB")
      if m:
        printmetric("read_views", m.group(1))
        continue
      m = match("(LOCK WAIT |)(\d+) lock struct")
      if m:
        counters["lock_structs"] = int(m.group(2)) + counters["lock_structs"]
        if m.group(1):
          counters["locked_transactions"] = counters["locked_transactions"] + 1
        continue
      m = match("(\d+) queries inside InnoDB, (\d+) queries in queue")
      if m:
        printmetric("queries_inside", m.group(1))
        printmetric("queries_queued", m.group(2))
        continue
    except Exception, e:
      err("Error processiong innodb data: %s" % e)
  
  # Aggregated metrics
  for key, value in counters.iteritems():
    printmetric(key, value)


def collect(db):
  """Collects and prints stats about the given DB instance."""

  ts = now()
  def printmetric(metric, value, tags=""):
    print "mysql.%s %d %s schema=%s%s" % (metric, ts, value, db.dbname, tags)

  has_innodb = False
  if db.isShowGlobalStatusSafe():
    global_status = db.query("SHOW GLOBAL STATUS")
    for metric, value in global_status:
      try:
        if "." in value:
          value = float(value)
        else:
          value = int(value)
      except ValueError:
        continue
      metric = metric.lower()
      has_innodb = has_innodb or metric.startswith("innodb")
      printmetric(metric, value)

  if has_innodb:
    collectInnodbStatus(db)

  if has_innodb and False:  # Disabled because it's too expensive for InnoDB.
    waits = {}  # maps a mutex name to the number of waits
    ts = now()
    for engine, mutex, status in db.query("SHOW ENGINE INNODB MUTEX"):
      if not status.startswith("os_waits"):
        continue
      m = re.search("&(\w+)(?:->(\w+))?$", mutex)
      if not m:
        continue
      mutex, kind = m.groups()
      if kind:
        mutex += "." + kind
      wait_count = int(status.split("=", 1)[1])
      waits[mutex] = waits.get(mutex, 0) + wait_count
    for mutex, wait_count in waits.iteritems():
      printmetric("innodb.locks", wait_count, " mutex=" + mutex)

  ts = now()

  mysql_slave_status = db.query("SHOW SLAVE STATUS")
  if mysql_slave_status:
    slave_status = todict(db, mysql_slave_status[0])
    master_host = slave_status["master_host"]
  else:
    master_host = None

  mysql_master_logs = db.query("SHOW MASTER LOGS")
  if mysql_master_logs:
     master_logs = todict(db, mysql_master_logs[0])

  if master_host and master_host != "None":
    sbm = slave_status.get("seconds_behind_master")
    if isinstance(sbm, (int, long)):
      printmetric("slave.seconds_behind_master", sbm)
    printmetric("slave.bytes_executed", slave_status["exec_master_log_pos"])
    printmetric("slave.bytes_relayed", slave_status["read_master_log_pos"])
    printmetric("slave.thread_io_running",
                isyes(slave_status["slave_io_running"]))
    printmetric("slave.thread_sql_running",
                isyes(slave_status["slave_sql_running"]))
    printmetric("slave.relay_log_space", slave_status["relay_log_space"])

  if master_logs:
    # Total binlog space usage
    if "file_size" in master_logs:
      total_size=0
      for row in mysql_master_logs:
        row = todict(db, row)
        total_size += row["file_size"]
      printmetric("binary_log_space", total_size)

  states = {}  # maps a connection state to number of connections in that state
  for row in db.query("SHOW PROCESSLIST"):
    id, user, host, db_, cmd, time, state = row[:7]

    # MySQL 5.5 replaces the 'Locked' state with a variety of "Waiting for
    # X lock" types of statuses.  Wrap these all back into "Locked" because
    # we don't really care about the type of locking it is.
    if state and re.match('^(Locked|Table lock|Waiting for .*lock)$',state):
      cmd = "locked"
    elif state and re.match('^User lock$',state):
      cmd = "user_locked"

    states[cmd] = states.get(cmd, 0) + 1
  for state, count in states.iteritems():
    state = state.lower().replace(" ", "_")
    printmetric("connection_states", count, " state=%s" % state)


def main(args):
  """Collects and dumps stats from a MySQL server."""
  while True:
    if find_sockfiles():
      break
    else:
      time.sleep(COLLECTION_INTERVAL)
      continue

# if not find_sockfiles():  # Nothing to monitor.
#   return 13               # Ask tcollector to not respawn us.
  if MySQLdb is None:
    err("error: Python module `MySQLdb' is missing")
    return 1

  utils.drop_privileges()
  last_db_refresh = now()
  dbs = find_databases()
  while True:
    ts = now()
    if ts - last_db_refresh >= DB_REFRESH_INTERVAL:
      find_databases(dbs)
      last_db_refresh = ts

    errs = []
    for dbname, db in dbs.iteritems():
      try:
        collect(db)
      except (EnvironmentError, EOFError, RuntimeError, socket.error,
              MySQLdb.MySQLError), e:
        if isinstance(e, IOError) and e[0] == errno.EPIPE:
          # Exit on a broken pipe.  There's no point in continuing
          # because no one will read our stdout anyway.
          return 2
        err("error: failed to collect data from %s: %s" % (db, e))
        errs.append(dbname)

    for dbname in errs:
      del dbs[dbname]

    sys.stdout.flush()
    time.sleep(COLLECTION_INTERVAL)


if __name__ == "__main__":
  sys.stdin.close()
  sys.exit(main(sys.argv))
