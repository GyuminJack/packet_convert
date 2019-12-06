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

def make_tsv(worker_id, file_name, host_ip, mac_dhcp_dict):
    program_path = "python3 /home/pi/packet_convert/src/tsv_preprocessing.py"
    
    for i in host_ip:
        print("WORKER({}) : (START) IP : {} ".format(worker_id, i),flush=True)
        try:
            each_mac = mac_dhcp_dict[i]
            cmd = "{} --file_name {} --host_ip {} --resampling_seconds 1 --multi multi".format(program_path,file_name, i)
            outname = subprocess.Popen(cmd, stdout=subprocess.PIPE,shell=True).stdout.read().decode().replace("\n","")

            if len(outname) > 5:
                mv_folder = '/home/pi/packet_convert/convert_to_tsv'
                rename = "{},{}".format(each_mac,",".join(outname.split("/")[-1].split(",")[1:]))
                move2 = subprocess.Popen("sudo mv {} {}/prepros_finish/{}/{}".format(outname,mv_folder,each_mac, rename), stdout=subprocess.PIPE,shell=True)
                print("WORKER({}) : (SAVED) MAC:{}, IP:{}, FILE:{} ".format(worker_id, i, each_mac,rename),flush=True)
            else:
                print("WORKER({}) : (NODATA) cmd : {}".format(worker_id, cmd)) 
        except:
            print("WORKER({}) : (ERROR) {}의 맥정보가 존재하지 않습니다.".format(worker_id,i))
    move3 = subprocess.Popen("sudo mv {} {}/original/original_tsv/".format(file_name, mv_folder), stdout=subprocess.PIPE,shell=True)
    print("WORKER({}) : (COMPLETE) {} ".format(worker_id, file_name),flush=True)
    print("-"*20)
    return outname

def mac_dhcp_read():
    mac_dhcp_string = subprocess.Popen("sudo cat /var/lib/misc/dnsmasq.leases",stdout=subprocess.PIPE, shell=True).stdout.read().decode()
    mac_ip_dict = dict()
    if len(mac_dhcp_string)>0:
        mac_dhcp_list = mac_dhcp_string.split("\n")
        for each_line in mac_dhcp_list:
            each_line_list = each_line.split(" ")
            if len(each_line_list) > 2:
                mac_ip_dict[each_line_list[2]] = each_line_list[1]
    
    mac_ip_dict["192.168.203.229"] = "SMU_device"
    return mac_ip_dict

class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
    def exit_gracefully(self,signum, frame):
        print("현재작업 이후 프로세스가 종료 됩니다.",flush=True)
        self.kill_now = True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_jobs", help="number of process",type=int, required=True)
    parser.add_argument("--host_ip", help="host ip path", type=str, required=True)
    args = parser.parse_args()
    

    mac_dhcp_dict = mac_dhcp_read()
    root_folder = '/home/pi/packet_convert'
    host_ips = subprocess.Popen("cat {}/bin/{}".format(root_folder,args.host_ip),stdout=subprocess.PIPE,shell=True).stdout.read().decode().split("\n")[:-1]
    for i in host_ips:
        try: 
            if os.path.isdir("{}/convert_to_tsv/prepros_finish/{}".format(root_folder,mac_dhcp_dict[i])):
                pass
            else:
                os.mkdir("{}/convert_to_tsv/prepros_finish/{}".format(root_folder,mac_dhcp_dict[i]))
        except:
            host_ips.remove(i)
            print("{}에 대한 mac정보가 존재하지 않아 Resampling에서 제외됩니다.".format(i))

    killer = GracefulKiller()
    while not killer.kill_now:
        time.sleep(0.2)
        try:
            file_list = last_file()[:args.n_jobs]
        except:
            file_list = last_file()
        if len(file_list)>0:
            print("현재 작업 파일 리스트 : {}".format(file_list),flush=True)
            procs = []
            for i, _file in enumerate(file_list):
                file_path = '/home/pi/packet_convert/convert_to_tsv/tsv_finish/'+_file
                print("WORKER({}) : {}".format(i, _file),flush=True)
                proc = Process(target = make_tsv, args=(i, file_path, host_ips, mac_dhcp_dict))
                procs.append(proc)
                proc.start()
            for proc in procs:
                proc.join()
            
    print("프로세스가 종료됩니다.")
   
