echo $(ps -ef | egrep `cat run.pid | tr '\n' '|' |  head --bytes -1` | awk '{print $2}')
sudo kill -2 $(ps -ef | egrep `cat run.pid | tr '\n' '|' |  head --bytes -1` | awk '{print $2}')


