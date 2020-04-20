import signal
import traceback
import math
import argparse
import time
import subprocess
import sys
import os
import pandas as pd
from dateutil.parser import parse
from datetime import timedelta, datetime
from multiprocessing import Process, Manager
from collections import Counter
signal.signal(signal.SIGINT, signal.SIG_IGN)

def extract_first_end_time(file_name, gap):
    first_time =  subprocess.Popen("head -2 {}".format(file_name), stdout=subprocess.PIPE,shell=True).stdout.read().decode().split("\n")[1].split("\t")[0]
    end_time = subprocess.Popen("tail -1 {}".format(file_name), stdout=subprocess.PIPE,shell=True).stdout.read().decode().replace("\n","").split("\t")[0]
    
    try:
        total_seconds = (parse(end_time)-parse(first_time)).total_seconds()
        total_trial = int(total_seconds / gap)+1
        first_time = datetime.strptime(first_time, "%m,%d,%H:%M:%S.%f")
        end_time = datetime.strptime(end_time, "%m,%d,%H:%M:%S.%f")
        return first_time, end_time, total_trial

    except:
        subprocess.Popen("sudo mv {} /home/pi/packet_convert/convert_to_tsv/etc/nothing_in_tsv/".format(file_name),shell=True)
      

def date_for_find(time, gap):
    tmp_time = time
    last_time = time + timedelta(seconds = gap)
    time_list = []
    while tmp_time < last_time :
        day_str = ",".join([str(tmp_time.month).zfill(2),str(tmp_time.day).zfill(2)])
        time_str = ":".join([str(tmp_time.hour).zfill(2),str(tmp_time.minute).zfill(2),str(tmp_time.second).zfill(2)])
        full_str = ",".join([day_str,time_str])
        tmp_time = tmp_time + timedelta(seconds=1)
        time_list.append(full_str)
    return time_list

def extract_one_second_statistics(sub_tmp, host_ip,  wht_ip, blk_ip, wn_port, wn_protocol):
    def mk_d_flag(ip, host_ip):
        if ip == host_ip :
            flag = 1
        elif len(ip) > 15:
            flag = -1
        else:
            flag = 0
        return flag

    def ip_check(contents, check_list):
        if contents in check_list:
            return 1
        else:
            return 0 

    def protocol_counter(protocol_list, wn_protocol):
        protocol_counter = Counter(protocol_list)
        return_list = []
        for i in wn_protocol:
            return_list.append(protocol_counter[i])
        return_list.append(sum(protocol_counter.values())-sum(return_list))
        return return_list
    
    tmp_stats = []
    column_name = ['time','protocol','src_ip','dst_ip','src_port','dst_port','length']
    tmp_df = pd.DataFrame(sub_tmp, columns=column_name)
    
    tmp_df['direction_flag'] = tmp_df['src_ip'].apply(lambda x: mk_d_flag(x, host_ip))

    tmp_df['wht_src_ip'] = tmp_df['src_ip'].apply(lambda x : ip_check(x, wht_ip))
    tmp_df['blk_src_ip'] = tmp_df['src_ip'].apply(lambda x : ip_check(x, blk_ip))
    tmp_df['wht_dst_ip'] = tmp_df['dst_ip'].apply(lambda x : ip_check(x, wht_ip))
    tmp_df['blk_dst_ip'] = tmp_df['dst_ip'].apply(lambda x : ip_check(x, blk_ip))
    counter_ip = list(tmp_df[['wht_src_ip','blk_src_ip','wht_dst_ip','blk_dst_ip']].apply(sum, axis=0))
                        
    protocol_count = protocol_counter(tmp_df['protocol'],wn_protocol)
    for i, item in enumerate(counter_ip):
        if math.isnan(item) == True:
            counter_ip[i]=0

    for i in [1,0,-1]:
        sub_df = tmp_df[tmp_df['direction_flag'] == i]['length'].apply(int)
        if len(sub_df)>0:
            one_sec_count = len(sub_df)
            one_sec_max_min = max(sub_df)-min(sub_df)
            one_sec_sum = sum(sub_df)
            tmp_stats += [one_sec_count,one_sec_max_min,one_sec_sum]
        else:
            tmp_stats += [0,0,0]
    tmp_stats +=  counter_ip      
    tmp_stats += protocol_count
    return tmp_stats


def single_main(file_name, start_time,total_trial, gap, wht_ip, blk_ip, wn_port, wn_protocol):
    total_list = []
    for i in range(total_trial):
        first_time = start_time + timedelta(seconds = gap*i)
        grep_date = date_for_find(first_time, gap)
        sub_tmp_list = []
        for subset in grep_date:
            grep_cmd = "cat {} | grep '{}' | grep '{}' ".format(file_name, subset, host_ip)
            try:
                sub_tmp = subprocess.Popen(grep_cmd, shell=True, stdout=subprocess.PIPE).stdout.read().decode().split("\n")[:-1]
                sub_tmp_list += [i.split("\t") for i in sub_tmp]
            except:
                continue
        stats_tmp = extract_one_second_statistics(sub_tmp_list, host_ip,wht_ip, blk_ip, wn_port, wn_protocol)
        total_list.append([grep_date[0]]+stats_tmp)
    return total_list

def list_split(int_trial, split):
    l = range(int_trial)
    n = math.ceil(int_trial/split)
    # return => [range(0, 25), range(25, 50), range(50, 75), range(75, 100)]
    return [l[i:i+n] for i in range(0, len(l), n)]

def multi_main(file_name, start_time, total_trial_list, gap, wht_ip, blk_ip, wn_port, wn_protocol, L):
    total_list = []
    for i in total_trial_list:
        first_time = start_time + timedelta(seconds = gap*i)
        grep_date = date_for_find(first_time, gap)
        sub_tmp_list = []
        for subset in grep_date:
            grep_cmd = "cat {} | grep '{}' | grep '{}' ".format(file_name, subset, host_ip)
            try:
                sub_tmp = subprocess.Popen(grep_cmd, stdout=subprocess.PIPE,shell=True).stdout.read().decode().split("\n")[:-1]
                sub_tmp_list += [i.split("\t") for i in sub_tmp]
            except:
                continue
        stats_tmp = extract_one_second_statistics(sub_tmp_list, host_ip, wht_ip, blk_ip, wn_port, wn_protocol)
        L.append([i,grep_date[0]]+stats_tmp)
    return total_list

def write_file(protocol_list,result,output_name, multi=False):
    print(output_name) # This is for second job.. Don't Remove.
    with open(output_name, "w") as f:
        if multi==True:
            f.write("seq_number\t")
        f.write("\t".join(['time','outbound(count)','outbound(max-min)','outbound(sum)','inbound(count)','inbound(max-min)','inbound(sum)','unknown(count)','unknown(max-min)','unknown(sum)','wht_src_ip','blk_src_ip','wht_dst_ip','blk_dst_ip']+protocol_list+['ukn_protocol','\n']))
        for i in result:
            for j in i:
                f.write(str(j))
                f.write("\t")
            f.write("\n")
        
def get_output_file_path(host_ip, file_name, resampling_second):
    root_path = '/home/pi/packet_convert/convert_to_tsv/prepros_finish/'
    file_name = host_ip+","+str(resampling_second)+","+file_name.split("/")[-1]
    output_file_name = os.path.join(root_path, file_name)
    return output_file_name

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_name',help='file path',required=True)
    parser.add_argument('--host_ip',required=True, type=str)
    parser.add_argument('--resampling_seconds',required=True, type=int)
    parser.add_argument('--multi', required=True, type=str)
    
    args = parser.parse_args()
    file_name = args.file_name
    host_ip = args.host_ip
    resampling_second = args.resampling_seconds
    multi = args.multi

    output_file_name = get_output_file_path(host_ip, file_name, resampling_second)

    try:
        known_protocol = ['SSDP','MDNS','STP','DHCP','ICMP','LSD','BROWSER','RTSP','DHCPv6','IGMPv2','TLSv1.2',
                          'ARP','NBNS','SSH','ICMPv6','CDP','UDP','TCP','LLMNR','DB-LSP-DISC','LLDP']
        start_time, _, total_trial = extract_first_end_time(file_name, int(resampling_second))
        if multi == 'multi':
            start_time, _, total_trial = extract_first_end_time(file_name, resampling_second)
            with Manager() as manager:
                L = manager.list()
                process_list = []
                splited_timeranges = list_split(total_trial,1)
                for each_timerange in splited_timeranges:
                    p = Process(target=multi_main, args=(file_name, start_time, each_timerange, resampling_second, [],[],[], known_protocol, L))
                    process_list.append(p)
                    p.start()
                for proc in process_list:
                    proc.join()
               
                write_file(known_protocol, list(L), output_file_name, multi=True)
        else:
            output = single_main(file_name, start_time ,total_trial, resampling_second , [],[],[], known_protocol)
            write_file(known_protocol,output,output_file_name)
    except:
        continue
        #traceback.print_exc()

