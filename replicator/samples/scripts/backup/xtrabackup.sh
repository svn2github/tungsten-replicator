#!/bin/bash -eu
# -e: Exit immediately if a command exits with a non-zero status.
# -u: Treat unset variables as an error when substituting.
# 
# Custom backup script that follows conventions for script backups implemented
# by ScriptBackupAgent.  
#

usage() {
  echo "Usage: $0 {-backup|-restore} -properties file [-options opts]"
}

# Parse arguments. 
operation=
options=
properties=

user=root
password=
host=localhost
port=3306
directory=/tmp/innobackup
archive=/tmp/innobackup.tar.gz
mysql_service_command="/etc/init.d/mysql"
mysqldatadir=/var/lib/mysql
mysqlibdatadir=""
mysqliblogdir=""
mysqllogdir=/var/lib/mysql
mysqllogpattern=mysql-bin
mysqluser=mysql
mysqlgroup=mysql
my_cnf=/etc/my.cnf

while [ $# -gt 0 ]
do
  case "$1" in
    -backup) 
      operation="backup";;
    -restore) 
      operation="restore";;
    -properties)
      properties="$2"; shift;;
    -options)
      options="$2"; shift;;
    *)  
      echo "unrecognized option: $1"
      usage;
      exit 1;
  esac
  shift
done

#
# Break apart the options and assign them to variables
#
for i in `echo $options | tr '&' '\n'`
do
  parts=(`echo $i | tr '=' '\n'`)
  numparts=${#parts[@]}

  if [ $numparts -gt 1 ]
  then
  	eval $parts=${parts[1]}
  fi
done

# Unset e to avoid bombing out if service command not in path. 
set +e
service_command=`which service`
set -e
if [ ! -x "$service_command" ];
then
  if [ -x "/etc/init.d/mysqld" ];
  then
    mysql_service_command="/etc/init.d/mysqld"
  elif [ -x "/etc/init.d/mysql" ];
  then
    mysql_service_command="/etc/init.d/mysql"
  fi
else
  if [ ! -x "$mysql_service_command" ];
  then
    echo "Unable to determine the service command to start/stop mysql" >&2
    exit 1
  fi
fi

# Ensure my.cnf is set and can be found. 
if [ ! -f "$my_cnf" ]; 
then
  echo "Unable to determine location of MySQL my.cnf file" >&2
  exit 1
fi

# Handle operation. 
if [ "$operation" = "backup" ]; then
  # Echo backup file to properties. 
  echo "file=$archive" > $properties

  # Clean up the filesystem before starting
  rm -rf $directory
  rm -f $archive

  # Copy the database files and apply any pending log entries
  innobackupex-1.5.1 --user=$user --password=$password --host=$host --port=$port --no-timestamp --defaults-file=$my_cnf $directory
  innobackupex-1.5.1 --apply-log --user=$user --password=$password --host=$host --port=$port --defaults-file=$my_cnf $directory

  # Package up the files and remove the staging directory
  cd $directory
  tar -czf $archive *
  rm -rf $directory

  exit 0
elif [ "$operation" = "restore" ]; then
  if [ ! -d "$mysqllogdir" ]; then
    echo "The MySQL log dir '$mysqllogdir' is not a directory" >&2
    exit 1
  fi
  
  if [ ! -d "$mysqldatadir" ]; then
    echo "The MySQL data dir '$mysqldatadir' is not a directory" >&2
    exit 1
  fi
  
  # Get the name of the backup file and restore.  
  . $properties

  # Clean up the filesystem before starting
  rm -rf $directory
  mkdir $directory
  cd $directory
  
  # Unpack the backup files
  tar -xzf $file

  # Stop mysql and clear the mysql data directory
  $mysql_service_command stop 1>&2

  # We are expecting the exit code to be 3 so we have to turn off the 
  # error trapping
  set +e
  `mysql -u$user -p$password -h$host -P$port -e "select 1"` > /dev/null 2>&1
  if [ $? -ne 1 ]; then
    echo "Unable to properly shutdown the MySQL service" >&2
    exit 1
  fi
  set -e
  
  rm -rf $mysqldatadir/*
  rm -rf $mysqllogdir/$mysqllogpattern.*

  # Copy the backup files to the mysql data directory
  innobackupex-1.5.1 --ibbackup=xtrabackup_51 --defaults-file=$my_cnf --copy-back $directory

  # Fix the permissions and restart the service
  chown -RL $mysqluser:$mysqlgroup $mysqldatadir
  
  if [ "$mysqlibdatadir" != "" ]; then
	chown -RL $mysqluser:$mysqlgroup $mysqlibdatadir
  fi

  if [ "$mysqliblogdir" != "" ]; then
	chown -RL $mysqluser:$mysqlgroup $mysqliblogdir
  fi

  $mysql_service_command start 1>&2

  # Remove the staging directory
  rm -rf $directory
  
  exit 0
else
  echo "Must specify -backup or -restore"
  usage
  exit 1
fi