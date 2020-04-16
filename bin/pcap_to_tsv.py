import subprocess
from dateutil.parser import parse
import argparse
import time

def pcap_to_csv(pcap_file):
    tmp = subprocess.Popen("tshark -r {} -T fields -e frame.time -e _ws.col.Protocol -e ip.src -e ip.dst -e tcp.srcport -e tcp.dstport -e frame.len -E header=y".format(pcap_file), shell=True, stdout=subprocess.PIPE).stdout.read()
    return tmp

if __name__ == "__main__":
    start_time = time.time()

    parser = argparse.ArgumentParser()
    parser.add_argument("--pcap", help="pcap file path", required=True)
    args = parser.parse_args()
    try:
        pcap_name = args.pcap
        a = pcap_to_csv(pcap_name).decode()
        with open("{}_5tuple.tsv".format(pcap_name[:-5]), "w") as f:
            for ln, line in enumerate(a.split("\n")):
                if ln == 0:
                    f.write(line)
                    f.write("\n")
                elif len(line) == 0:
                    pass
                else:
                    tmp_line = line.replace("\n","").split("\t")
                    f.write("\t".join([parse(tmp_line[0]).strftime("%m,%d,%H:%M:%S.%f")]+tmp_line[1:]))
                    f.write("\n")
        print(time.time()-start_time)
    except:
        print("USAGE : python3 pcap_to_tsv.py pcapfile.pacp")
