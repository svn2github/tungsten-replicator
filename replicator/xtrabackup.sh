#!/bin/bash
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
directory=/tmp/innobackup
archive=/tmp/innobackup.tar

service=mysql
mysqldatadir=/var/lib/mysql
mysqluser=mysql
mysqlgroup=mysql

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
  eval $parts=${parts[1]}
done

# Handle operation. 
if [ "$operation" = "backup" ]; then
  # Echo backup file to properties. 
  echo "file=$archive" > $properties

  # Clean up the filesystem before starting
  rm -rf $directory
  rm -f $archive

  # Copy the database files and apply any pending log entries
  innobackupex-1.5.1 --user=$user --password=$password --no-timestamp $directory
  innobackupex-1.5.1 --apply-log --user=$user --password=$password $directory

  # Package up the files and remove the staging directory
  cd $directory
  tar -cf $archive *
  rm -rf $directory

elif [ "$operation" = "restore" ]; then
  # Get the name of the backup file and restore.  
  . $properties

  # Clean up the filesystem before starting
  rm -rf $directory
  mkdir $directory
  cd $directory
  
  # Unpack the backup files
  tar -xf $file

  # Stop mysql and clear the mysql data directory
  service $service stop
  rm -rf $mysqldatadir/*

  # Copy the backup files to the mysql data directory
  innobackupex-1.5.1 --copy-back $directory

  # Fix the permissions and restart the service
  chown -R $mysqluser:$mysqlgroup $mysqldatadir
  service $service start

  # Remove the staging directory
  rm -rf $directory
else
  echo "Must specify -backup or -restore"
  usage
  exit 1
fi
