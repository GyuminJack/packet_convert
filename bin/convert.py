import subprocess
import argparse
from multiprocessing import Process
import time
import signal


def last_file():
    packet_dir = '/home/pi/packet_convert'
    file_list_time = subprocess.Popen("ls {}/raw_pcap/ -Art".format(packet_dir), stdout=subprocess.PIPE,shell=True).stdout.read()
    if len(file_list_time.decode())>1:
        file_list_time = file_list_time.decode().split("\n")[:-1]
    else:
        file_list_time = []
    return file_list_time

def make_tsv(file_name):
    time.sleep(3)
    program_path = "python3 /home/pi/packet_convert/src/pcap_to_tsv.py"
    root_dir = '/home/pi/packet_convert'
    status = subprocess.Popen("{} --pcap {}/raw_pcap/{}".format(program_path,root_dir,file_name),stdout=subprocess.PIPE ,shell=True).stdout.read()
    move1 = subprocess.Popen("sudo mv {}/raw_pcap/{} {}/convert_to_tsv/original/original_packet/".format(root_dir,file_name, root_dir),stdout=subprocess.PIPE,shell=True).stdout.read()
    move2 = subprocess.Popen("sudo mv {}/raw_pcap/{}_5tuple.tsv {}/convert_to_tsv/tsv_finish".format(root_dir,file_name[:-5], root_dir),stdout=subprocess.PIPE , shell=True).stdout.read()
    
    print("{} ok".format(file_name),flush=True)
    
    return status


class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
    def exit_gracefully(self, signum, frame):
        print("현재 작업 이후 프로세스가 종료됩니다",flush=True)
        self.kill_now = True




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_jobs", help="number of process",type=int, required=True)
    args = parser.parse_args()
    
    print("Pcap을 Tsv로 변환합니다.",flush=True)
    killer = GracefulKiller()
    while not killer.kill_now:
        time.sleep(0.5)
        try:
            file_list = last_file()[:args.n_jobs]
        except:
            file_list = last_file()
        if len(file_list)>0:
            print("현재 작업 파일 리스트 : {}".format(file_list),flush=True)
            procs = []
            for i, _file in enumerate(file_list):
                print("서브 프로세스 : {}".format(_file),flush=True)
                proc = Process(target = make_tsv, args=(_file,))
                procs.append(proc)
                proc.start()
            for proc in procs:
                proc.join()
        else:
            pass
   
