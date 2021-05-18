#!/bin/bash

bash compile.sh
bash run_test_harness.sh workloads/small
./build/test/tester

