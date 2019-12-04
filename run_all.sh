. /home/pi/py37_env/bin/activate
sh ./bin/run_packet_capture.sh > ./log/tcpdump.log 2>&1 & echo $!>> ./log/run.pid;
sh ./bin/run_pcap_to_tsv.sh > ./log/pcap_to_tsv.log 2>&1 & echo $!>> ./log/run.pid ;
sh ./bin/run_resample_tsv.sh > ./log/resample.log 2>&1 & echo $!>> ./log/run.pid
