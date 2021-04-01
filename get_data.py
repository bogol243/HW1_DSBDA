#!/usr/bin/python3
import argparse
from string import Template
from datetime import date,datetime
from random import randint

parser = argparse.ArgumentParser()
parser.add_argument("num_lines")
parser.add_argument("num_files")
parser.add_argument("num_broken")
parser.add_argument("date_begin")
parser.add_argument("date_end")
args = parser.parse_args()
#print(args.num_lines)

trash_string = '*'*8+'#'*11
log_string = Template('$level: $date dm-pc kernel:[ 3719.190261] audit: type=1400 audit(1616751714.042:55630): apparmor="DENIED" operation="ptrace" profile="snap.discord.discord" pid=5878 comm="Discord" requested_mask="read" denied_mask="read" peer="unconfined"')

num_broken = int(args.num_broken)
num_lines = int(args.num_lines)
num_files = int(args.num_files)
date_begin = datetime.fromisoformat(args.date_begin)
date_end = datetime.fromisoformat(args.date_end)

level = lambda: randint(0,7)
level_counter = [0]*8
level_dict = {7:"debug",
    6:"info",
    5:"notice",
    4:"warning",
    3:"error",  
    2:"critical",
    1:"alert",
    0:"panic"}


res = []
count_broken=0
for n in range(num_lines):
    if(n == int(num_lines/num_broken)*count_broken):
        res.append(trash_string)
        count_broken+=1
        continue
    date_cur_timestamp = randint(date_begin.timestamp(),date_end.timestamp())
    date_cur = datetime.fromtimestamp(date_cur_timestamp)
    level_cur = level()
    level_counter[level_cur]+=1
    log_cur = log_string.substitute(level=level_cur,date=str(date_cur))
    res.append(str(log_cur))

for n in range(num_files):
    f = open('input/syslog.'+str(n),'w')
    for line in res[n:len(res):num_files]:
        f.write(line+"\n")
    f.close()

for (level_num,level_name) in level_dict.items():
    print(level_name+": "+str(level_counter[level_num]))

print(f"num broken lines:{count_broken}")
