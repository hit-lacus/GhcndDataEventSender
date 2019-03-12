cd source
rm -f *.data
python metadata_reader.py
ls -lh *.data
python data_sender.py --data-path "/root/lacus/data/ghcnd/*.csv.gz" \
    --sleep-millsecond-per-thousand 0 --output-format csv
cd ../ddl
ls -lh *.data
sh -x upload_hdfs.sh