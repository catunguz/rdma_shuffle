#!/bin/bash

cd "$(dirname "$0")"

if [ ! -f distexprunner/ ]; then
    git clone https://github.com/mjasny/distexprunner/
    cd ./distexprunner
    git reset --hard 2c53850
    cd ..
fi

python3 distexprunner/client.py -vv exp.py