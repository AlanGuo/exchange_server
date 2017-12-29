#!/bin/bash

killall -s SIGQUIT monitorcenter.exe
sleep 1
./monitorcenter.exe config.json
