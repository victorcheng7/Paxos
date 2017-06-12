#!/bin/bash
function runTests {
    for f in $1/*.in; do
	filename=$(basename "$f")
	filename="${filename%.*}"
	./asg2_debug $filename $1/setup $f > $1/$filename.debug &
    done
}

function printDebugLogs {
    for f in $1/*.debug; do
	filename=$(basename "$f")
	filename="${filename%.*}"
	echo -----------------------------
	echo DEBUG LOG FOR SITE $filename:
	echo -----------------------------
	cat $f
    done
}

function runAllTests {
    let num_test_cases=1
    for d in ./tests/*; do
	if [[ ! -d $d ]]; then
	    continue
	fi
	echo =====================================TESTCASE $num_test_cases=============================================
	runTests $d
	while ps | grep -q python; do
	    sleep 1
	done
	printDebugLogs $d
	num_test_cases=$[num_test_cases+1]
    done
}

if [ $# -eq 0 ]; then
    runAllTests
else
    if [[ -d $1 ]]; then
	runTests $1
	while ps | grep -q python; do
	    sleep 1
	done
	printDebugLogs $1
    else
	runTests tests/test$1/
	while ps | grep -q python; do
	    sleep 1
	done	
	printDebugLogs tests/test$1/
    fi
fi 
