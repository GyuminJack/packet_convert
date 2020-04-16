import shutil
import time
import glob
import os
import subprocess

device_names = glob.glob("*")
device_names.remove("mover.py")
print(device_names)
i = 2
device_name = device_names[i]
files = glob.glob(os.path.join(device_name,"*.tsv"))
for file_name in files:
    if "_" in device_name:
        date = file_name.split("/")[1].split("_")[2]
    else:
        date = file_name.split("/")[1].split("_")[1]

    if not os.path.isdir(os.path.join(".",file_name.split("/")[0],date)):
        print(os.path.join(".",file_name.split("/")[0],date))
        os.makedirs(os.path.join("./",file_name.split("/")[0],date))
    print("src : ",file_name)
    print("dst : ",os.path.join(".",file_name.split("/")[0],date))
    shutil.move(file_name, os.path.join(".",file_name.split("/")[0],date))
