import subprocess
import argparse
from multiprocessing import Process
import time,os,signal
def last_file():
    file_list_time = subprocess.Popen("ls /home/pi/packet_convert/convert_to_tsv/tsv_finish -r",stdout=subprocess.PIPE , shell=True).stdout.read()
    if len(file_list_time.decode())>1:
        file_list_time = file_list_time.decode().split("\n")[:-1]
    else:
        file_list_time = []
    return file_list_time

def make_tsv(file_name, host_ip):
    program_path = "python3 /home/pi/packet_convert/src/tsv_preprocessing.py"
    for i in host_ip:
        cmd = "{} --file_name {} --host_ip {} --resampling_seconds 1 --multi multi".format(program_path,file_name, i)
        outname = subprocess.Popen(cmd, stdout=subprocess.PIPE,shell=True).stdout.read().decode().replace("\n","")
        mv_folder = '/home/pi/packet_convert/convert_to_tsv'
        move2 = subprocess.Popen("sudo mv {} {}/prepros_finish/{}/".format(outname,mv_folder,i), stdout=subprocess.PIPE,shell=True)
    move3 = subprocess.Popen("sudo mv {} {}/original/original_tsv/".format(file_name, mv_folder), stdout=subprocess.PIPE,shell=True)
    print("작업 완료 : {}".format(file_name))
    return outname
    
def preexec_function():
    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
    def exit_gracefully(self,signum, frame):
        print("현재작업 이후 프로세스가 종료 됩니다.")
        self.kill_now = True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_jobs", help="number of process",type=int, required=True)
    parser.add_argument("--host_ip", help="host ip path", type=str, required=True)
    args = parser.parse_args()
    root_folder = '/home/pi/packet_convert'
    host_ips = subprocess.Popen("cat {}/bin/{}".format(root_folder,args.host_ip),stdout=subprocess.PIPE,shell=True).stdout.read().decode().split("\n")[:-1]
    for i in host_ips:
        if os.path.isdir("{}/convert_to_tsv/prepros_finish/{}".format(root_folder,i)):
            pass
        else:
            os.mkdir("{}/convert_to_tsv/prepros_finish/{}".format(root_folder,i))
            os.mkdir("{}/convert_to_tsv/prepros_finish/{}/training_data".format(root_folder,i))
    print("Tsv에 대한 전처리를 시작합니다.")
    killer = GracefulKiller()
    while not killer.kill_now:
        time.sleep(0.2)
        try:
            file_list = last_file()[:args.n_jobs]
        except:
            file_list = last_file()
        if len(file_list)>0:
            print("현재 작업 파일 리스트 : {}".format(file_list))
            procs = []
            for i, _file in enumerate(file_list):
                file_path = '/home/pi/packet_convert/convert_to_tsv/tsv_finish/'+_file
                print("서브 프로세스 : {}".format(file_path))
                proc = Process(target = make_tsv, args=(file_path,host_ips))
                procs.append(proc)
                proc.start()
            for proc in procs:
                proc.join()
            
    print("프로세스가 종료됩니다.")
   
