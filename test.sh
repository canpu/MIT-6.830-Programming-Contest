#!/bin/bash

bash build.sh
bash run_test_harness.sh workloads/small
./build/test/tester

