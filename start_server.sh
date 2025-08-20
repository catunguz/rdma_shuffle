#!/bin/bash

cd "$(dirname "$0")"

if [ ! -f distexprunner/ ]; then
    git clone https://github.com/mjasny/distexprunner/
    cd ./distexprunner
    git reset --hard 2c53850
    cd ..
fi

# Do not modify the port calculation!
python3 distexprunner/server.py -vv --port $((20000 + $(id -u)))
