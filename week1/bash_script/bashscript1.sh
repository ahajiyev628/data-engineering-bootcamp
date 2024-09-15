#! /bin/bash

directory=$1

for file in "$directory"/*; do
    if [ -d "$file" ]; then
        size=$(du -sh "$file" | awk '{print $1}')
        echo "The size of the directory $file: $size"
    elif [ -f "$file" ]; then
        size=$(ls -lh "$file" | awk '{print $5}')
        echo "The size of the file $file: $size"
    fi
done
