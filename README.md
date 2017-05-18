# GlobalSnapshot
CS171 Assignment Global Snapshot
---

## TO DO:
  * ~~Fix testOutput script to adapt to this task~~
  * ~~Fix run.sh script to adapt to this task~~
  * ~~Adding the snap_id hashtable for the algorithm~~
  * ~~Handle the error of processes finishing faster before all other processes finish setting up~~
  * ~~Handdle the error of having more messages in Queue than Buffer Size Allow~~

---

## TO RUN:
There are 2 files that will execute for you:
  1. `./run.sh`: This script will run all test suite in tests subdirectory and tell you whether they pass or not. If failed, an expected vs. output will be printed
  2. `./run_debug`: There are 3 ways of runing this script
     * `./run_debug`: Run this script with no argument will run all the test suites in tests subdirectory and output the debug log of each site onto the screen
     * `./run_debug [test_suite_directory]`: Run the test suite at the `[test_suite_directory]` and output the debug log of each site onto the screen
       * E.g: `./run_debug tests/test1/`
     * `./run_debug [test_suite_number]`: Run the test suite with the number specified and output the debug log of each site onto the screen (Behaved same as the above)
       * E.g: `./run_debug 1`

---

To run: 
1) Create setup file and command file, then run this for every process
./asg2 [site_id] [setup_file] [command_file]

Author: Thien Hoang (7832413), Victor Cheng (3900552)