hadoop-scripts
==============

A group of scripts useful for managing a hadoop cluster.

bin/hdfs_du

      Simple wrapper around fs -du command to make it more human readable.

bin/hdfs_tmp_cleaner.pl

      A tool to automate cleaning out of the /tmp inside HDFS.  This only
      looks at the top-level directory structure for the file and directory
      timestamps.  It will not recursively descend within subdirectories of
      /tmp.

bin/mapred_find_stuck_tasks
    
      A tool to find and kill MapReduce v1 jobs stuck throwing 
      "Error launching tasks" errors in Cloudera Hadoop.
      

