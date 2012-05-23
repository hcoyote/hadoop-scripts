#!/usr/bin/perl
#===============================================================================
#
#         FILE:  hdfs_tmp_cleaner.pl
#
#        USAGE:  ./hdfs_tmp_cleaner.pl
#           --rm-batch  - disable interactive prompting
#           --keep-days - number of days to keep in /tmp
#
#  DESCRIPTION:
#
#      OPTIONS:  ---
# REQUIREMENTS:  ---
#         BUGS:  ---
#        NOTES:  ---
#       AUTHOR:  Travis Campbell (), <hcoyote@ghostar.org>
#      COMPANY:
#      VERSION:  1.0
#      CREATED:  05/14/12 17:16:57 CDT
#     REVISION:  ---
#===============================================================================

use strict;
use warnings;


use IO::File;
use Time::ParseDate;
use Getopt::Long;


my $opt_rm_batch;
my $opt_hdfs_path =  "hdfs://localhost:9000";
my $opt_keep_days = 2;

GetOptions(
    'rm-batch' => \$opt_rm_batch,
    'keep-days=i' => \$opt_keep_days,
    'help' => sub { print "$0
        --rm-batch  = delete stuff without prompting for cofnirmation
        --keep-days = number of days back to keep (default: $opt_keep_days)
        --help      = see --help for more information\n\n";
        exit;

        },

);


# don't change the number of seconds in a day until proven that earth has slowed down
# due to friction.  Mental note:  check back in a few millenia.
my $DAYS_IN_SECONDS = 86400;
my $KEEP_DAYS = $opt_keep_days;

# Things we need to work in HDFS; let's also limit the deletions to /tmp for now.
my $path = "/tmp";
my $hadoop_cmd = "/usr/bin/hadoop";
my $hadoop_ls  = "$hadoop_cmd fs -ls ";
my $hadoop_rmr = "$hadoop_cmd fs -rmr ";
my $hadoop_du  = "$hadoop_cmd fs -du ";
my $hadoop_dus = "$hadoop_cmd fs -du -s ";


my $total_size = 0;
my $total_delete_size = 0;

my %hdfs_path_info;

# When is now and who knows it?
my $date = parsedate("now");

my $fh = IO::File->new("$hadoop_ls $path|") or die "Could not open $hadoop_ls: $!";
my $dufh = IO::File->new("$hadoop_du $path|") or die "Could not open $hadoop_du: $!";

# grab the directory size for reporting later.
while (<$dufh>) {
    next if /^Found \d+ items/;

    chomp;

    my ($du_size, $path) = split(/\s+/);

    $path =~ s/$opt_hdfs_path//;

    $hdfs_path_info{$path}{du} = $du_size;
}


# grab the list of file/directory metadata.
while (<$fh>) {
    # skip a header
    next if /^Found \d+ items/;

    # nom nom the \r\n's!
    chomp;

    my ($mode, $replicas, $user, $group, $filesize, $mod_date, $mod_time, $path) = split(/\s+/);

    # give us seconds from epoch
    my $file_time = parsedate("$mod_date $mod_time");

    # get file age
    my $dur = $date - $file_time;

    # if we're looking at directories, we want the du size in our totals, otherwise, filesize.
    if ($mode =~ /^d/) {
        $total_size += $hdfs_path_info{$path}{du};
    } else {
        $total_size += $filesize;
    }

    # if our file/dir is older than our threshhold, nuke it from LEO.
    if ($dur > ($KEEP_DAYS * $DAYS_IN_SECONDS)) {
        print "$mod_date $mod_time $path is > $KEEP_DAYS (DELETE CANDIDATE => " . $hdfs_path_info{$path}{du}  . " bytes)\n";
        if ($mode =~ /^d/) {
            $total_delete_size += $hdfs_path_info{$path}{du};
        } else {
            $total_delete_size += $filesize;
        }

        $hdfs_path_info{$path}{deleteme} = 1;

    } else {
        print "$mod_date $mod_time $path is < $KEEP_DAYS\n";
    }


}

# Tell me what I'm about to delete so I know how much space will roughly free up in the DFS.
print "TOTAL DELETE CANDIDATES = " . $total_delete_size / (1024 * 1024 * 1024 ) . " gigabytes\n";
print "TOTAL SIZE              = " . $total_size / (1024 * 1024 * 1024) . "gigabytes\n";

# let's actually work on deleting things.
foreach my $path (sort keys %hdfs_path_info) {
    if (exists $hdfs_path_info{$path}{deleteme} and $hdfs_path_info{$path}{deleteme} == 1) {

        # Go interactive unless we're deleting in batch mode.
        if (not defined $opt_rm_batch) {
            print "Ready to delete $path: ";

            my $prompt = <STDIN>;
            chomp $prompt;

            next unless ($prompt =~ /y/i) ;
        }

        # Delete it and point some stuff out.
        my $rmfh = IO::File->new("$hadoop_rmr $path|") or warn "Could not delete $path: $!";
        while (<$rmfh>) {
            if (/^Deleted/) {
                print "Deletion of $path successful\n";
            } else {
                print ;
            }
        }
    }
}
