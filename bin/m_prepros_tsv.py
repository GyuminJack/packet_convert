import subprocess
import argparse
from multiprocessing import Process
import datetime
import time
import os
import signal
import yaml

def last_file():
    file_list_time = subprocess.Popen("ls /home/pi/packet_convert/convert_to_tsv/tsv_finish -r",stdout=subprocess.PIPE , shell=True).stdout.read()
    sorted_files = []
    if len(file_list_time.decode())>1:
        sorted_files = file_list_time.decode().split("\n")[:-1]
    return sorted_files

def make_tsv(worker_id, file_name, host_ip, mac_dhcp_dict):
    program_path = "python3 /home/pi/packet_convert/bin/prepros_tsv.py"
    
    for i in host_ip:
        st_time = time.time()
        print("WORKER({}) : (START) IP : {} ".format(worker_id, i),flush=True)
        try:
            each_mac = mac_dhcp_dict[i]
            cmd = "{} --file_name {} --host_ip {} --resampling_seconds 1 --multi multi".format(program_path, file_name, i)
            outname = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).stdout.read().decode().replace("\n","")

            if len(outname) > 5:
                original_file = outname
                root_path = '/home/pi/packet_convert/convert_to_tsv'
                date_path = datetime.datetime.now().strftime("%Y-%m-%d")
                rename = "{},{}".format(each_mac,",".join(outname.split("/")[-1].split(",")[1:]))
                move_path = os.path.join(root_path, "prepros_finish", each_mac, date_path, rename)
                mv_to_prepros_finish = subprocess.Popen("sudo mv {} {}".format(original_file, move_path), stdout=subprocess.PIPE,shell=True)
                print("WORKER({}) : (SAVED) MAC:{}, IP:{}, FILE:{}, Time:{:.3f}s ".format(worker_id, i, each_mac, rename, time.time()-st_time),flush=True)
            else:
                print("WORKER({}) : (NODATA) cmd : {}".format(worker_id, cmd)) 
        except:
            print("WORKER({}) : (ERROR) {}의 맥정보가 존재하지 않습니다.".format(worker_id,i))
    to_original_path = os.path.join(root_path, "original","original_tsv/")
    mv_to_original_path = subprocess.Popen("sudo mv {} {}".format(file_name, to_original_path), stdout=subprocess.PIPE,shell=True)
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

def add_path_rm_hosts(host_ips):
    global mac_dhcp_dict
    root_folder = '/home/pi/packet_convert'
    date_path = datetime.datetime.now().strftime("%Y-%m-%d")
    for i in host_ips:
        try:
            print(i)
            mac_name = mac_dhcp_dict[i]
            create_path = os.path.join(root_folder, "convert_to_tsv", "prepros_finish", mac_name, date_path)
            if not os.path.isdir(create_path):
                os.makedirs(create_path)
        except:
            host_ips.remove(i)
            print("IP({})에 대한 mac정보가 존재하지 않아 Resampling에서 제외됩니다.".format(i))
            continue
    return host_ips

def read_host_ips(host_ip_path):
    with open(host_ip_path, "r") as f:
        host_ips = yaml.load(f, Loader = yaml.FullLoader)
    return host_ips


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n_jobs", help="number of process",type=int, required=True)
    parser.add_argument("--host_ip", help="host ip path", type=str, required=True)
    args = parser.parse_args()
    
    
    mac_dhcp_dict = mac_dhcp_read()
    print("인식된 맥 주소 : ",mac_dhcp_dict)
    host_ips = read_host_ips(args.host_ip)['host_ips']
    host_ips = add_path_rm_hosts(host_ips)
    print("host ip 목록 :",host_ips)
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
   
