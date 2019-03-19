import glob
import sys

if __name__ == "__main__":
    print "Args", sys.argv
    path_patterns = sys.argv[1]
    all_paths = glob.glob(path_patterns)
    year_list = []

    for p in all_paths:
        print "Search", p
        year_list.append(int(p[-11:-7]))

    with open("../ddl/upload_hdfs.sh", 'w') as hdfs_scripts:
        for y in year_list:
            print >> hdfs_scripts, "hadoop fs -nkdir -p /LacusDir/data/hive/ghcn/%d/" % y
            print >> hdfs_scripts, "hadoop fs -copyFromLocal ../source/fact_%d_* /LacusDir/data/hive/ghcn/%d-01-01/" % (y, y)

    with open("../ddl/alter_hive.sql", 'w') as alter_scripts:
        for y in year_list:
            print >> alter_scripts, "ALTER TABLE ghcn ADD IF NOT EXISTS PARTITION (part_year='%d-01-01') " \
                                    "LOCATION '/LacusDir/data/hive/ghcn/%d-01-01/';" % (y, y)
