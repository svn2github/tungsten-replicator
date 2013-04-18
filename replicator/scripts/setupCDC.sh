#!/bin/bash

# Unsetting variables
unset delete_publisher
unset delete_subscriber
unset delete_user

CNF_FILE="setupCDC.conf"

[ ! -z "${1}" ] && CNF_FILE=${1} && echo "Using configuration ${CNF_FILE}"

[ ! -f "${CNF_FILE}" ] && echo "ERROR: Configuration file '${CNF_FILE}' was not found" && exit 1
. ${CNF_FILE}

# Checking that delete_user is replaced by delete_publisher and delete_subscriber
if [ -z "${delete_publisher+undefined}" ] || [ -z "${delete_subscriber+undefined}" ] ; 
then
   # delete_publisher or delete_subscriber not found -> 
   # check if the conf file needs to be upgraded (i.e. if it still defines delete_user)
   if [ -z "${delete_user+undefined}" ];
   then
      echo delete_publisher and delete_subscriber are mandatory parameters. Please check they are defined in configuration file.;
      exit 1;
   else
      # delete_user is still defined in conf file -> conf file should be upgraded to newer version
      echo "delete_user parameter is deprecated! Please use delete_publisher={1,0} and delete_subscriber={1,0} instead.";
      exit 1;
   fi
fi

DEFAULT_CHANGE_SET="TUNGSTEN_CHANGE_SET"
CHANGE_SET=${DEFAULT_CHANGE_SET}
[ ! -z "${service}" ] && CHANGE_SET="TUNGSTEN_CS_${service}"

if [ -n "${sys_user}" ]
then
   if [ -n "${sys_pass}" ]
   then
      syspass=$sys_pass
   else
      read -s -p "Enter password for $sys_user, if any :" syspass;
   fi
else
   syspass=
fi
SYSDBA="$sys_user/$syspass AS SYSDBA"

oracle_version="`sqlplus -S ${SYSDBA} @get_oracle_version`" 

echo ""
echo "Configuring CDC for service '${service}' for Oracle ${oracle_version}. Change Set is '${CHANGE_SET}'"

echo "Removing old CDC installation if any (SYSDBA)"
if [ $oracle_version -ge 11 ]
then
   sqlplus -S -L ${SYSDBA} @cleanup_cdc_tables.sql $pub_user ${CHANGE_SET}
else
   sqlplus -S -L ${SYSDBA} @cleanup_cdc_tables-10.sql $pub_user ${CHANGE_SET}
fi
RC=$?
[ ${RC} -ne 0 ] && echo "ERROR: [$RC] sqlplus statement failed" && exit 1
echo "Done."

if [ $delete_publisher -eq 1 ]
then
   echo "Deleting old publisher user '$pub_user' (SYSDBA)"
   sqlplus -S -L ${SYSDBA} @delete_user.sql $pub_user
   if [ $? -eq 148 ]
   then
      echo "Disconnect publisher user (${pub_user}) connections."
      echo "Setup was incomplete!"
      exit $?
   else
      echo "Done."
   fi
fi # delete_publisher -eq 1

if [ $delete_subscriber -eq 1 ]
then
   echo "Deleting old subscriber/Tungsten user '$tungsten_user' (SYSDBA)"
   sqlplus -S -L ${SYSDBA} @delete_user.sql $tungsten_user
   RC=$?
   if [ ${RC} -eq 126 ]
   then
       echo "WARN: [$RC] sqlplus statement failed (Tungsten user did not exist?) - ignoring"
   elif [ ${RC} -ne 0 ]
   then
      echo "ERROR: [$RC] sqlplus statement failed" && exit 1
   fi
   echo "Done."
fi # delete_subscriber -eq 1

if [ $specific_tables -eq 1 ]
then
   if [ -n "${specific_path}" ]
   then
      specificpath="${specific_path}"
   else
      specificpath="`pwd`"
   fi

   if [ ! -r "$specificpath/tungsten.tables" ]
   then
      echo "File tungsten.tables cannot be read in $specificpath"
      exit 1
   fi
else
   specificpath="`pwd`"
fi

echo "Setup tungsten_load (SYSDBA)"
sqlplus -S -L ${SYSDBA} @create_tungsten_load.sql $specificpath $pub_user $specific_tables
RC=$?
[ ${RC} -ne 0 ] && echo "ERROR: [$RC] sqlplus statement failed" && exit 1
echo "Done."

echo "Creating publisher/subscriber and preparing table instantiation (SYSDBA)"
sqlplus -S -L ${SYSDBA} @create_publisher_subscriber.sql $source_user $pub_user $pub_password $tungsten_user $tungsten_pwd $cdc_type
RC=$?
[ ${RC} -ne 0 ] && echo "ERROR: [$RC] sqlplus statement failed" && exit 1
echo "Done."

echo "Setting up $cdc_type ($pub_user)"
sqlplus -S -L $pub_user/$pub_password @create_change_set.sql $source_user $cdc_type $tungsten_user $pub_user ${CHANGE_SET}
RC=$?
[ ${RC} -ne 0 ] && echo "ERROR: [$RC] sqlplus statement failed" && exit 1
echo "Done."

echo "adding synonym if needed ($tungsten_user)"
sqlplus -S -L $tungsten_user/$tungsten_pwd @create_synonym.sql $pub_user
RC=$?
[ ${RC} -ne 0 ] && echo "ERROR: [$RC] sqlplus statement failed" && exit 1
echo "Done."

echo "Cleaning up (SYSDBA)"
sqlplus -S -L ${SYSDBA} @drop_tungsten_load.sql
RC=$?
[ ${RC} -ne 0 ] && echo "ERROR: [$RC] sqlplus statement failed" && exit 1
echo "Done."

sqlplus -S $sys_user/$syspass AS SYSDBA @determine_capture_position.sql ${CHANGE_SET}
RC=$?
[ ${RC} -ne 0 ] && echo "ERROR: [$RC] sqlplus statement failed" && exit 1
