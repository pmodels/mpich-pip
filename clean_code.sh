#!/bin/bash

files=`git status -s | awk '{print $2}'`

for line in $files; do
	case $line in
		*ucx*)
			continue
			;;
		*libfabric*)
			continue
			;;
		*.sh)
			continue
			;;
		*.txt*)
			continue
			;;
		*clean_code*)
			continue
			;;
		*Makefile*)
			continue
			;;
		*)
			if [[ -d $line ]]; then
				continue
			fi
			;;
	esac

	echo "./maint/code-cleanup.sh $line"
	./maint/code-cleanup.sh $line
	
done
