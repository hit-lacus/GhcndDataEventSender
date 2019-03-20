import glob
import sys
import datetime

if __name__ == "__main__":
    print "Args", sys.argv
    path_patterns = sys.argv[1]
    all_paths = glob.glob(path_patterns)
    year_list = []

    partition_dt = datetime.datetime(2018, 1, 1)

    for p in all_paths:
        print "Search", p
        year_list.append(int(p[-11:-7]))

    with open("../ddl/upload_hdfs.sh", 'w') as hdfs_scripts:
        print >> hdfs_scripts, "hadoop fs -rm -r /LacusDir/data/hive/ghcn/"
        for y in year_list:
            partition_str = partition_dt.strftime("%Y-%m-%d")
            print >> hdfs_scripts, "hadoop fs -mkdir -p /LacusDir/data/hive/ghcn/%s/" % partition_str
            print >> hdfs_scripts, "hadoop fs -copyFromLocal ../fact_%d* /LacusDir/data/hive/ghcn/%s/" \
                                   % (y, partition_str)
            partition_dt = partition_dt + datetime.timedelta(days=1)

    partition_dt = datetime.datetime(2018, 1, 1)
    with open("../ddl/alter_hive.sql", 'w') as alter_scripts:
        print >> alter_scripts, "use lacus;"
        for y in year_list:
            partition_str = partition_dt.strftime("%Y-%m-%d")
            print >> alter_scripts, "ALTER TABLE ghcn ADD IF NOT EXISTS PARTITION (part_year='%s') " \
                                    "LOCATION '/LacusDir/data/hive/ghcn/%s/';" % (partition_str, partition_str)
            partition_dt = partition_dt + datetime.timedelta(days=1)
