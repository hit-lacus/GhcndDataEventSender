# hadoop fs -copyFromLocal ../source/1870.csv /LacusDir/data/hive/ghcn/1870

hadoop fs -rm -r /LacusDir/data/hive/ghcn/

hadoop fs -mkdir -p /LacusDir/data/hive/ghcn/2001/
hadoop fs -mkdir -p /LacusDir/data/hive/ghcn/1875/
hadoop fs -mkdir -p /LacusDir/data/hive/ghcn/1876/
hadoop fs -mkdir -p /LacusDir/data/hive/ghcn/1877/

hadoop fs -copyFromLocal ../source/1874.data /LacusDir/data/hive/ghcn/1874/
hadoop fs -copyFromLocal ../source/1875.data /LacusDir/data/hive/ghcn/1875/
hadoop fs -copyFromLocal ../source/1876.data /LacusDir/data/hive/ghcn/1876/
hadoop fs -copyFromLocal ../source/1877.data /LacusDir/data/hive/ghcn/1877/

# hadoop fs -copyFromLocal ../source/country.data /LacusDir/data/hive/
# hadoop fs -copyFromLocal ../source/station.data /LacusDir/data/hive/
# hadoop fs -copyFromLocal ../source/state.data /LacusDir/data/hive/

hive -f create.ddl
