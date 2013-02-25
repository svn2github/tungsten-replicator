#!/bin/bash 

set_tungsten_env() {
	export CONTINUENT_ROOT=@{HOME_DIRECTORY}
	if [ -z $CONTINUENT_ROOT ];
	then
		echo "you must have the environment variable CONTINUENT_ROOT defined"
		echo "and it must point at a valid, readable directory"
		return 1
	fi

	if [ ! -d $CONTINUENT_ROOT ];
	then
		echo "the current value for CONTINUENT_ROOT, $CONTINUENT_ROOT"
		echo "must point at a valid, readable directory"
		return 1
	fi

	export PATH=$PATH:$CONTINUENT_ROOT/tungsten/tungsten-replicator/bin:$CONTINUENT_ROOT/tungsten/cluster-home/bin:$CONTINUENT_ROOT/share:$CONTINUENT_ROOT/tungsten/tools@{ADDITIONAL_PATH}
	return 0
}

set_tungsten_env