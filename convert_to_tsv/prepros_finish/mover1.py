import shutil
import time
import glob
import os
import subprocess

device_names = glob.glob("*/")
device_names = [i.replace("/","") for i in device_names]
print(device_names)
i = 2
device_name = device_names[i]
files = glob.glob(device_name+"*.tsv")
for file_name in files:
    if "_" in device_name:
        date = file_name.split("_")[2]
    else:
        date = file_name.split("_")[1]
    mv_root = os.path.join(".",device_name,date)
    if not os.path.isdir(mv_root):
        os.makedirs(mv_root)
    print("src : ",file_name)
    print("dst : ",os.path.join(mv_root))
    shutil.move(file_name, mv_root)
