hadoop-scripts
==============

A group of scripts useful for managing a hadoop cluster.

bin/hdfs_du

      Simple wrapper around fs -du command to make it more human readable.

bin/hdfs_tmp_cleaner.py 

      A tool to automate cleaning out of the /tmp inside HDFS utilizing the
      snakebite library from Spotify.  Will operate in a recursive and non-
      recursive mode when looking through paths.  By default, it looks at
      just the top-level path to determine what should be deleted; if you
      need it to descend into the directory structure, you can enable that
      but it can significantly increase run time depending on how many files
      it has to work through.

      Works on Kerberized and HA HDFS clusters.

bin/hdfs_tmp_cleaner.pl (deprecated - only here for history)

      A tool to automate cleaning out of the /tmp inside HDFS.  This only
      looks at the top-level directory structure for the file and directory
      timestamps.  It will not recursively descend within subdirectories of
      /tmp.


bin/mapred_find_stuck_tasks
    
      A tool to find and kill MapReduce v1 jobs stuck throwing 
      "Error launching tasks" errors in Cloudera Hadoop.
      

