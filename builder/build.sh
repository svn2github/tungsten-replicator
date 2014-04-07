#!/bin/bash

##########################################################################
# NAME:  BUILD
#
# SUMMARY:  Build script for Tungsten Replicator
#
# OVERVIEW:
#   This script performs a full build of the replicator and all its
#   dependencies, plus bristlecone and cookbook tools.
#   It checks out and builds all components: replicator, commons.
#
#   Before building you should review properties in the 'config' file. If
#   you need to override them, do so in config.local (which is svn:ignored)
#   then run the script.
#
#   You can specify a different configuration file as an argument.
#   Otherwise, it defaults to "config" file.
#
# USAGE:
#    build [config-file]
#
##########################################################################

##########################################################################
# Source and define various functions
##########################################################################

source ./helpers.sh

# Creates the src tarball
build_source() {
  printHeader "Creating Replicator source release"

  # Copy sources to appropriate directory.  Tests are not included, so we
  # drop them.
  reldir_src=${reldir}-src
  echo "### Creating source release directory: ${reldir_src}"
  rm -rf ${reldir_src}
  mkdir -p ${reldir_src}

  echo "### Copying in source files"
  modules_sources_folder=$reldir_src/sources
  builder_folder=$reldir_src/builder
  mkdir $modules_sources_folder
  mkdir $builder_folder
  cp -r $SRC_DIR/replicator $modules_sources_folder
  cp -r $SRC_DIR/commons $modules_sources_folder
  cp -r $SRC_DIR/bristlecone $modules_sources_folder
  cp -r $SRC_DIR/cookbook $modules_sources_folder
  cp -r build.sh config extra $builder_folder/
  echo "SKIP_CHECKOUT=1" > $builder_folder/config.local

  # Clean all copied source trees to keep only necessary files
  echo "### Cleaning-up source folders"
  reldir_src_sources=${reldir_src}/sources
  doAnt commons ${reldir_src_sources}/commons/build.xml clean
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

source_commons=${SRC_DIR}/commons
source_bristlecone=${SRC_DIR}/bristlecone
source_replicator=${SRC_DIR}/replicator
source_community_extra=extra
source_cookbook=${SRC_DIR}/cookbook

extra_replicator=${source_community_extra}/replicator
extra_cluster_home=${source_community_extra}/cluster-home
extra_tools=${source_community_extra}/tools

jars_commons=${source_commons}/build/jars
lib_commons=${source_commons}/lib

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
if [ -z $SVN_USER ]; then
  export SVN_USER=anonymous
# else use $SVN_USER environment variable
fi
echo "OK to set code.google.com <http://code.google.com> SVN_USER to $SVN_USER?"
echo "Press enter to continue.  Otherwise quit and set SVN_USER in your environment"
read ignored_answer

if [ -z $SVN_USER ]; then
  export SVN_USER=`whoami`
  echo "OK to set code.google.com SVN_USER to $SVN_USER?"
  echo "Press enter to continue.  Otherwise quit and set SVN_USER in your environment"
  read ignored_answer
else
  export SVN_USER=anonymous
fi


# Release name.
product="Tungsten Replicator"
relname=tungsten-replicator-${VERSION}
# Add Jenkins build number if any
if [ "x${BUILD_NUMBER}" != "x" ]
then
    relname=${relname}-${BUILD_NUMBER}
fi

##########################################################################
# Check out files.
##########################################################################

printHeader "Source code preparation"

if [ ! -d ${BUILD_DIR} ]; then
  mkdir ${BUILD_DIR}
fi
if [ ! -d ${SRC_DIR} ]; then
  mkdir ${SRC_DIR}
fi

echo "### List of source locations"
echo "# Commons:    $source_commons"
echo "# Replicator: $source_replicator"
echo "# Bristlecone: $source_bristlecone"
echo "# Cookbook: $source_cookbook"

if [ ${SKIP_CHECKOUT} -eq 1 ]; then
  echo "### Using existing code without checkout"
else
  doSvnCheckout commons ${TCOM_SVN_URL} $source_commons
  doSvnCheckout replicator ${TREP_SVN_URL} $source_replicator
  doSvnCheckout bristlecone ${BRI_SVN_URL} $source_bristlecone
  doSvnCheckout cookbook ${COOK_SVN_URL} $source_cookbook
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
  doAnt commons $source_commons/build.xml clean dist 
  doAnt replicator $source_replicator/build.xml clean dist javadoc
  doAnt bristlecone $source_bristlecone/build.xml clean dist
fi

source ./build_tarball.sh
build_tarball

##########################################################################
# Create source build if desired.
##########################################################################

if [ "$SKIP_SOURCE" = 0 ]; then
  build_source
else
  echo "### Skipping source code release generation"
fi

##########################################################################
# Create tools build if desired.
##########################################################################

if [ ${SKIP_TOOLS} -eq 0 ]
then
  printHeader "Creating tools release"

  reldir_tools=${reldir}-tools
  echo "### Creating tools release directory: ${reldir_tools}"
  rm -rf ${reldir_tools}
  mkdir -p ${reldir_tools}
  mkdir -p ${reldir_tools}/.runtime

	cp $extra_tools/configure $reldir_tools
	cp $extra_tools/configure-service $reldir_tools
	cp $extra_tools/tungsten-installer $reldir_tools
	cp $extra_tools/update $reldir_tools
  rsync -Ca $extra_tools/ruby $reldir_tools
  rsync -Ca $extra_tools/ruby-tools-only/* $reldir_tools/ruby/

  if [ ${INCLUDE_TPM} -eq 1 ]
	then
		cp $extra_tools/tpm $reldir_tools
		rsync -Ca $extra_tools/ruby-tpm $reldir_tools
	  rsync -Ca $extra_tools/ruby-tools-only/* $reldir_tools/ruby-tpm/
	fi

  cp -rf ${reldir} ${reldir_tools}/.runtime
  cp -rf ${reldir}/.man* ${reldir_tools}
  
  rel_tools_tgz=${relname}-tools.tar.gz

  echo "### generating tools tar file: ${rel_tools_tgz}"
  (cd ${reldir}/..; tar -czf ${rel_tools_tgz} ${relname}-tools/)
else
  echo "### Skipping tools release generation"
fi
