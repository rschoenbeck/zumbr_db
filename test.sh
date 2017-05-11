#!/bin/bash
# Test tables for ZUMBR course project database

echo ""
echo "*** RUNNING COUNT AND SUM TESTS ***"

find tests -name \*.sql | while read sql_file; do
	echo "Running test '$sql_file'..."
	result=`psql -t -h $db_host -d $db_name -U $db_user -f $sql_file | awk '{$1=$1};1'`
	if [[ "$result" == "0" ]]
		then echo "PASS -- Test passed with discrepancy value $result"
		else echo "FAIL -- Test failed with discrepancy value $result"
	fi
done
