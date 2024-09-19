#! /bin/bash

cnt=$1
path_type=$2
file_name=$3

for ((i=1; i<=cnt; i++)); do
    if [ "$path_type" = "file" ]; then
        touch "${file_name}-${i}"
    elif [ "$path_type" = "folder" ]; then
        mkdir "${file_name}-${i}"
    fi
done

ls -l ${file_name}-*
