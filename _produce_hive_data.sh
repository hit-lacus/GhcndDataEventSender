#--------------------------------------------------------#
#--------------------------------------------------------#
#--------------------------------------------------------#

sh -x setenv.sh
echo "RawFile at "$raw_file

echo "清除上次生成的数据文件"
rm -f fact_*

cd source
echo "生成维度表数据"
python metadata_reader.py
echo "生成事实表数据"
python data_sender.py --data-path "$raw_file" \
    --sleep-millsecond-per-thousand 0 --output-format csv




#--------------------------------------------------------#
#--------------------------------------------------------#
#--------------------------------------------------------#
cd ..
ls -l fact_*

# 获取事实表数据文件列表
datafiles=`ls fact_*`

echo "遍历事实表数据文件，并且进行切割"
for single_f in $datafiles
do
 echo "split "$single_f;
 split -a 4 -d -l 100000 $single_f $single_f"-";
 rm -f $single_f;
done

ls -l fact_*


echo "创建 上传HDFS文件脚本和增加hive Partition脚本"
cd source
python create_scripts.py "$raw_file"


#--------------------------------------------------------#
#--------------------------------------------------------#
#--------------------------------------------------------#
cd ../ddl
echo "上传 数据文件"
sh -x upload_hdfs.sh
echo  "创建 Hive表"
hive -f create.ddl
echo "增加 Table Partition"
hive -f alter_hive.sql
echo "检查数据"
hive -f check_result.sql

