#! /bin/bash
btree_init mydisk $1 $2 $3;
#btree_insert mydisk 1 9 9;
#MAX = 30;

for (( i=$4; i >0; i--))
    do
        btree_insert mydisk $1 $i $i;
    done
btree_display mydisk $1 normal;
