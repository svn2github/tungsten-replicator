#!/bin/bash

##########################################################################
# NAME:  RELEASE-REPLICATOR 
#
# SUMMARY:  Build script for Tungsten Replicator
#
# OVERVIEW:  
#   This script performs a full build of the replicator and all its 
#   dependencies, plus bristlecone tool.
#   It checks out and builds all components, including the replicator itself,
#   this to avoid any un-commited code from being included in a release.
#
#   Before building you should review and if necessary edit properties in 
#   the 'config' file, overriding them in config.local if necessary, then 
#   run the script.  
#
#   You can specify a different configuration file as an argument.  
#   Otherwise, it defaults to "config" file. 
# 
# USAGE:  
#    release-community [config-file]
#
##########################################################################


##########################################################################
# Helper routines.  
##########################################################################

# Print a nice header for output. 
printHeader() {
  echo 
  echo "################################################################"
  echo "# $1"
  echo "################################################################"
  echo
}

# Check out code and exit if unsuccessful. 
doSvnCheckout() {
  local component=$1
  local url=$2
  local source_dir=$3
  echo "### Checking out component: $component"
  echo "# SVN URL: $url"
  echo "# Source directory: $source_dir"
  svn checkout $url $source_dir
  if [ "$?" != "0" ]; then
    echo "!!! SVN checkout failed!"
    exit 1
  fi
}

# Run an ant build and exit if it fails. 
doAnt() {
  local component=$1; shift
  local build_xml=$1; shift
  local targets=$*
  echo "### Building component: $component"
  echo "# Ant build.xml: $build_xml"
  echo "# Ant targets:   $targets"
  ant -f $build_xml $targets
  if [ "$?" != "0" ]; then
    echo "!!! Ant build failed"
    exit 1
  fi
}

# Copy from the component build location to the release. 
doCopy() {
  local component=$1
  local build_src_dir=$2
  local build_tgt_dir=$3
  echo "### Copying component: $component"
  echo "# Build source: $build_src_dir"
  echo "# Build target: $build_tgt_dir"
  cp -r $build_src_dir $build_tgt_dir/$component
  if [ "$?" != "0" ]; then
    echo "!!! Copy operation failed"
    exit 1
  fi
}

# Fix wrapper binary path in the given file to use centralized binary 
fixWrapperBin() {
  ${SED_DASH_I} 's/^WRAPPER_CMD=.*/WRAPPER_CMD=\"..\/..\/cluster-home\/bin\/wrapper\"/' $1
}

# Tries to locate the command passed as first argument. If the command is not
# found, exists with a comprehensive error message
checkCommandExists() {
  which $1 > /dev/null 2>&1
  if [ $? -eq 0 ]; then
    echo "# $1 found."
  else
    echo "!!! Required $1 executable not found, please install it! Exiting."
    exit 1
  fi
}

# Removes the given 1st arg library pattern and copy from 2nd arg jar to 
# either cluster-home/lib/ or to the 3rd arg dir if given
cleanupLib() {
  find ${reldir} -name $1 -exec \rm -f {} \; > /dev/null 2>&1
  if [ x$3 == "x" ]
  then
    cp $2 $cluster_home/lib
  else
    cp $2 $3
  fi
}

##########################################################################
# Handle arguments.
##########################################################################

if [ ! -z $1 ]; then
  if [ ! -r $1 ]; then
    echo "!!! Unknown or unreadable configuration file: $1"
    exit 1
  fi
  config=$1
fi

##########################################################################
# Initialization and cautions to user. 
##########################################################################

if [ -z $config ]; then
  config=config
fi

printHeader "REPLICATOR BUILD SCRIPT"

echo "Did you update config.local? (press enter to continue)"
read ignored_answer

source ./$config

if [ -f config.local ]; then
  echo "Overriding $config with config.local"
  source config.local
fi

##########################################################################
# Set global variables. 
##########################################################################

source_commons=${BUILD_DIR}/commons
source_fsm=${BUILD_DIR}/fsm
source_bristlecone=${BUILD_DIR}/bristlecone
source_replicator=${BUILD_DIR}/replicator
source_enterprise_builder=${BUILD_DIR}/enterprise-builder
source_community_extra=${BUILD_DIR}/extra

extra_replicator=${source_community_extra}/replicator
extra_cluster_home=${source_community_extra}/cluster-home
extra_tools=${source_community_extra}/tools

##########################################################################
# Handle platform differences.  This script works on MacOSX & Linux.
##########################################################################

# Prevents creation of '._' files under Max OS/X
export COPYFILE_DISABLE=true

# Fixes the problem with differing sed -i command in Linux and Mac
SEDBAK_EXT=.sedbak
if [ "`uname -s`" = "Darwin" ]
then
  SED_DASH_I="sed -i $SEDBAK_EXT"
else
  SED_DASH_I="sed -i$SEDBAK_EXT"
fi

##########################################################################
# Environment checks.
##########################################################################
echo "### Checking environment..."
checkCommandExists svn
checkCommandExists ant
echo "### ... Success!"


##########################################################################
# Additional initializations
##########################################################################

# SVN user
if [ $ENTERPRISE_BUILD -eq 1 ] && [ -z $SVN_USER ]; then 
  export SVN_USER=`whoami`
  echo "OK to set forge.continuent.org SVN_USER to $SVN_USER?"
  echo "Press enter to continue.  Otherwise quit and set SVN_USER in your environment"
  read ignored_answer
else
  export SVN_USER=anonymous
fi

# Release name.
relname=tungsten-replicator-${VERSION}

##########################################################################
# Check out files. 
##########################################################################

printHeader "Source code preparation"

if [ ! -d ${BUILD_DIR} ]; then
  mkdir ${BUILD_DIR}
fi

echo "### List of source locations"
echo "# Commons:    $source_commons"
echo "# FSM:        $source_fsm"
echo "# Replicator: $source_replicator"
echo "# Bristlecone: $source_bristlecone"

if [ ${SKIP_CHECKOUT} -eq 1 ]; then
  echo "### Using existing code without checkout"
else
  doSvnCheckout fsm ${TFSM_SVN_URL} $source_fsm
  doSvnCheckout commons ${TCOM_SVN_URL} $source_commons
  doSvnCheckout replicator ${TREP_SVN_URL} $source_replicator
  doSvnCheckout bristlecone ${BRI_SVN_URL} $source_bristlecone
  doSvnCheckout community_extra ${EXTRA_SVN_URL} $source_community_extra

  # Pull in Enterprise code
  if [ $ENTERPRISE_BUILD -eq 1 ]; then
    doSvnCheckout enterprise-builder ${EBLD_SVN_URL} $source_enterprise_builder
  fi
fi

##########################################################################
# Build products.  
##########################################################################

if [ ${SKIP_BUILD} -eq 1 ]
then
  printHeader "Using existing builds"
else
  printHeader "Building replicator from source"
  # Run the builds. 
  doAnt commons $source_commons/build.xml clean releases
  doAnt fsm $source_fsm/build.xml clean releases
  doAnt replicator $source_replicator/build.xml clean releases
  doAnt bristlecone $source_bristlecone/build.xml clean releases
  if [ $ENTERPRISE_BUILD -eq 1 ]; then
    doAnt enterprise_replicator $source_enterprise_builder/extra/replicator/build.xml clean dist
  fi
  
fi

##########################################################################
# Copy files into the community build. 
##########################################################################

printHeader "Creating Replicator release"
reldir=${BUILD_DIR}/${relname}
if [ -d $reldir ]; then
  echo "### Removing old release directory"
  \rm -rf $reldir
fi

echo "### Creating release: $reldir"
mkdir -p $reldir

# Copy everything!
doCopy tungsten-replicator $source_replicator/build/tungsten-replicator $reldir 
doCopy bristlecone $source_bristlecone/build/dist/bristlecone $reldir 

##########################################################################
# Fix up replicator files. 
##########################################################################

reldir_replicator=$reldir/tungsten-replicator
replicator_bin=$reldir_replicator/bin/replicator
replicator_wrapper_conf=$reldir_replicator/conf/wrapper.conf

echo "### Replicator: pointing to centralized Java Service Wrapper binaries"

${SED_DASH_I} 's/^wrapper.java.classpath.1.*/wrapper.java.classpath.1=..\/..\/tungsten-replicator\/lib\/*.jar/' ${replicator_wrapper_conf}
${SED_DASH_I} 's/^wrapper.java.classpath.2.*/wrapper.java.classpath.2=..\/..\/tungsten-replicator\/conf/' ${replicator_wrapper_conf}
${SED_DASH_I} 's/^wrapper.java.classpath.3.*/wrapper.java.classpath.3=..\/lib\/wrapper*.jar\
wrapper.java.classpath.4=..\/..\/cluster-home\/lib\/*.jar/' ${replicator_wrapper_conf}
${SED_DASH_I} 's/^wrapper.java.library.path.1=..\/lib\/wrapper/wrapper.java.library.path.1=..\/lib/' ${replicator_wrapper_conf}
${SED_DASH_I} 's/^wrapper.java.additional.1=-Dreplicator*/wrapper.java.additional.1=-Dreplicator.home.dir=..\/..\/tungsten-replicator/' ${replicator_wrapper_conf}
${SED_DASH_I} 's/^wrapper.java.additional.2=-Dreplicator*/wrapper.java.additional.2=-Dreplicator.log.dir=..\/..\/tungsten-replicator\/log/' ${replicator_wrapper_conf}
${SED_DASH_I} 's/^wrapper.logfile=..*/wrapper.logfile=..\/..\/tungsten-replicator\/log\/trepsvc.log/' ${replicator_wrapper_conf}

fixWrapperBin $replicator_bin

##########################################################################
# Create the cluster home. 
##########################################################################

echo "### Creating cluster home"
cluster_home=$reldir/cluster-home
mkdir -p $cluster_home/conf/cluster

mkdir -p $cluster_home/bin
echo "# Moving wrapper binaries"
\mv $reldir_replicator/bin/wrapper* $cluster_home/bin/
chmod 755 $cluster_home/bin/* > /dev/null 2>&1
echo "# Moving wrapper libraries"
mkdir -p $cluster_home/lib
\mv $reldir_replicator/lib/wrapper/* $cluster_home/lib/
echo "# Moving wrapper libraries"

echo "### Adding configuration files"
echo "# Adding and setting permissions for configure script"
cp $extra_cluster_home/configure $reldir
chmod 755 $reldir/configure
cp $extra_cluster_home/configure-service $reldir
chmod 755 $reldir/configure-service

echo "# Copying in Ruby configuration libraries"
cp -r $extra_cluster_home/lib $cluster_home

echo "### Creating tools"
tools_dir=$reldir/tools
mkdir -p $tools_dir
cp $extra_tools/configure $tools_dir
rsync -Ca $extra_tools/ruby $tools_dir
rsync -Ca $extra_tools/ruby-community/ $tools_dir/ruby
rsync -Ca $extra_tools/ruby-full-package/ $tools_dir/ruby

##########################################################################
# Create manifest file. 
##########################################################################

manifest=${reldir}/.manifest
echo "### Creating manifest file: $manifest" 

echo "# Build manifest file" >> $manifest
echo "DATE: `date`" >> $manifest
echo "RELEASE: ${relname}" >> $manifest
echo "USER ACCOUNT: ${USER}" >> $manifest

# Hudson environment values.  These will be empty in local builds. 
echo "BUILD_NUMBER: ${BUILD_NUMBER}" >> $manifest
echo "BUILD_ID: ${BUILD_NUMBER}" >> $manifest
echo "JOB_HAME: ${JOB_NAME}" >> $manifest
echo "BUILD_TAG: ${BUILD_TAG}" >> $manifest
echo "HUDSON_URL: ${HUDSON_URL}" >> $manifest
echo "SVN_REVISION: ${SVN_REVISION}" >> $manifest

# Local values. 
echo "HOST: `hostname`" >> $manifest
echo "SVN URLs:" >> $manifest
echo "  ${TCOM_SVN_URL}" >> $manifest
echo "  ${TFSM_SVN_URL}" >> $manifest
echo "  ${TREP_SVN_URL}" >> $manifest
echo "  ${BRI_SVN_URL}" >> $manifest

echo "SVN Revisions:" >> $manifest
echo -n "  commons: " >> $manifest
svn info $source_commons | grep Revision: >> $manifest
echo -n "  fsm: " >> $manifest
svn info $source_fsm | grep Revision: >> $manifest
echo -n "  replicator: " >> $manifest
svn info $source_replicator | grep Revision: >> $manifest
echo -n "  bristlecone: " >> $manifest
svn info $source_bristlecone | grep Revision: >> $manifest

if [ $ENTERPRISE_BUILD -eq 1 ]; then
  extra_ent_replicator=$source_enterprise_builder/extra/replicator
  reldir_replicator=$reldir/tungsten-replicator

  echo "### Adding extra bin scripts for replicator plugins"
  cp $extra_ent_replicator/bin/pg-* $reldir_replicator/bin

  echo "### Adding extra conf files for PostgreSQL"
  cp $extra_ent_replicator/conf/sample.* $reldir_replicator/conf

  echo "### Copying in Ruby replication plugin libraries"
  cp -r $extra_ent_replicator/lib/ruby $reldir_replicator/lib

  echo "### Copying in Python replication plugin libraries"
  cp -r $extra_ent_replicator/lib/python $reldir_replicator/lib

  echo "### Copying in extra sample scripts"
  cp -r $extra_ent_replicator/samples $reldir_replicator

  echo "### Removing old protobuf libraries"
  mv $reldir_replicator/lib/protobuf-java-2.2.0.jar $reldir_replicator/lib/protobuf-java-2.2.0.jar.old

  echo "Adding enterprise information to manifest file"
  echo "Enterprise SVN URL:" >> $manifest
  echo "  ${EBLD_SVN_URL}" >> $manifest
  echo -n "Enterprise SVN " >> $manifest
  svn info $source_enterprise_builder | grep Revision: >> $manifest
fi

cat $manifest

echo "### Cleaning up left over files"
# find and delete directories named .svn and any file named *<sed extension>
find ${reldir} \( -name '.svn' -a -type d -o -name "*$SEDBAK_EXT" \) -exec \rm -rf {} \; > /dev/null 2>&1

##########################################################################
# Generate tar file. 
##########################################################################
rel_tgz=${relname}.tar.gz
echo "### Creating tar file: ${rel_tgz}"
(cd ${reldir}/..; tar -czf ${rel_tgz} ${relname})

##########################################################################
# Create source build for community release only
##########################################################################

if [ "$SKIP_SOURCE" = 0 ]; then
  printHeader "Creating Replicator source release"

  # Copy sources to appropriate directory.  Tests are not included, so we 
  # drop them. 
  reldir_src=${reldir}-src
  echo "### Creating source release directory: ${reldir_src}"
  rm -rf ${reldir_src}
  mkdir -p ${reldir_src}
  
  echo "### Copying in source files"
  modules_sources_folder=$reldir_src/sources
  mkdir $modules_sources_folder
  cp -r $BUILD_DIR/replicator $modules_sources_folder
  cp -r $BUILD_DIR/commons $modules_sources_folder
  cp -r $BUILD_DIR/fsm $modules_sources_folder
  cp -r $BUILD_DIR/bristlecone $modules_sources_folder

  # Clean all copied source trees to keep only necessary files
  echo "### Cleaning-up source folders"
  reldir_src_sources=${reldir_src}/sources
  doAnt commons ${reldir_src_sources}/commons/build.xml clean
  doAnt fsm ${reldir_src_sources}/fsm/build.xml clean
  doAnt replicator ${reldir_src_sources}/replicator/build.xml clean
  doAnt bristlecone ${reldir_src_sources}/bristlecone/build.xml clean

  # Remove svn folders from source distrib
  echo "### Cleaning-up extra svn information"
  find $reldir_src -name ".svn" -exec \rm -rf {} \; > /dev/null 2>&1

  echo "### Copying-in manifest"
  # Use the same manifest as for bin distrib
  cp $manifest $reldir_src/

  rel_src_tgz=${relname}-src.tar.gz

  echo "### generating source tar file: ${rel_src_tgz}"
  (cd ${reldir}/..; tar -czf ${rel_src_tgz} ${relname}-src/)
else
  echo "### Skipping source code release generation"
fi
