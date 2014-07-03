#!/usr/bin/python3
## lsf-convert Copyright 2014 Timothy Middelkoop License Apache 2.0

data=open('lsb.acct',encoding="latin-1")
out=open('lewis-log.csv','w',encoding="utf-8")

limit=False
#limit=100000

import csv
import datetime

### Data structures
## http://www-01.ibm.com/support/knowledgecenter/SSETD4_9.1.2/lsf_config_ref/lsb.acct.5.dita

### Example data from text file.
'''
------------------------------------------------------------------------------

Job <101>, Job Name <test155>, User <sanders>, Project <default>, Status <EXIT>
                     , Queue <normal>, Command <#BSUB -J test155;#BSUB -n 1;#BS
                     UB -R "span[hosts=1] && bigmem";#BSUB -oo test155.o%J;#BSU
                     B -eo test155.e%J;time run_g03 test155.com>
Fri Apr 23 14:42:54: Submitted from host <lewis2.rnet.missouri.edu>, CWD <$HOME
                     /gaussian>, Output File (overwrite) <test155.o%J>, Error F
                     ile (overwrite) <test155.e%J>;
Fri Apr 23 14:43:00: Dispatched to <comp-002>;
Fri Apr 23 14:43:00: Completed <exit>.

Accounting information about this job:
     CPU_T     WAIT     TURNAROUND   STATUS     HOG_FACTOR    MEM    SWAP
      0.02        6              6     exit         0.0035     2M     25M
'''

### Example data from lsb.acct (one line)
'''
"JOB_FINISH" "7.06" 1272051780 101 201 33816627 1 1272051774 0 0 1272051780 
"sanders" "normal" "span[hosts=1] && bigmem" "" "" "lewis2.rnet.missouri.edu" 
"gaussian" "" "test155.o%J" "test155.e%J" "1272051774.101" 0 1 "comp-002" 32 60.0 
"test155" 
"#BSUB -J test155;#BSUB -n 1;#BSUB -R ""span[hosts=1] && bigmem"";#BSUB -oo test155.o%J;#BSUB -eo test155.e%J;time run_g03 test155.com"
0.006998 0.013997 0 0 -1 0 0 1854 0 0 0 0 -1 0 0 0 65 12 -1 "" 
"default" 32512 1 "" "" 0 2080 25396 "" "" "" "" 0 "" 0 "" -1 "" "" "" "" -1 "" "" 12589072 "" 
1272051780 "" "" 0
'''

jobs=[]
meta=list()
first=True ## Collect metadata only once.
keepAll=False

class Job:
    number=None
    tag=None
    
    def __init__(self):
        self.tag={}
    
    def __repr__(self):
        return "<%d: %s" % (self.number,str(self.tag))
        
    def add(self,tag,value,keep=True,blank=False,date=False):
        if not keep and not keepAll:
            return value
        if blank and (value=='' or value==0 or value=='0'):
            value=None
        if date:
            if value=='0':
                value=None
            else:
                value=str(datetime.datetime.fromtimestamp(int(value)))
        self.tag[tag]=value
        if first:
            meta.append(tag)
        return value
    
    def addMany(self,line,header,floating=True):
        for h in header:
            v=line.pop(0)
            if floating==True:
                v=float(v)
            self.add(h,v)

    def extract(self,line):
        
        #print("##",line)
        assert line.pop(0)=='JOB_FINISH'
        assert line.pop(0)=='7.06' # structure version 

        self.add('time',line.pop(0),date=True)
        self.number=self.add('Job',int(line.pop(0)))

        self.add('uid',int(line.pop(0)))
        self.add('options',line.pop(0))
        self.add('cores',int(line.pop(0)))

        self.add('submit',line.pop(0),date=True)
        self.add('begin',line.pop(0),date=True)
        self.add('term',line.pop(0),date=True)
        self.add('start',line.pop(0),date=True)

        self.add('User',line.pop(0))
        self.add('Queue',line.pop(0))

        self.add('resources',line.pop(0))
        self.add('dependency',line.pop(0))

        self.add('preExecCmd',line.pop(0),False)
        self.add('fromHost',line.pop(0),False)
        self.add('cwd',line.pop(0))
        
        self.add('inFile',line.pop(0),False)
        self.add('outFile',line.pop(0),False)
        self.add('errFile',line.pop(0),False)
        self.add('jobFile',line.pop(0),False)

        ## Asked
        n=self.add('numAskedHosts',int(line.pop(0)))
        hosts=[]
        for _ in range(0,n):
            hosts.append(line.pop(0))
        self.add('askedHosts','|'.join(hosts),True)
        
        ## Executed
        n=self.add('numExHosts',int(line.pop(0)))
        hosts=[]
        for _ in range(0,n):
            hosts.append(line.pop(0))
        self.add('execHosts','|'.join(hosts),True)

        ## Exit Status
        v=self.add('jStatus',int(line.pop(0)))
        assert v==32 or v==64
        self.add('hostFactor',float(line.pop(0)))
        
        self.add('jobName',line.pop(0))
        self.add('command',line.pop(0),False)
        
        ## Resource Usage
        self.addMany(line,['ru_utime','ru_stime','ru_maxrss','ru_ixrss','ru_ismrss','ru_idrss','ru_isrss',
                           'ru_minflt','ru_majflt','ru_nswap','ru_inblock','ru_oublock','ru_ioch',
                           'ru_msgsnd','ru_msgrcv','ru_nsignals','ru_nvcsw','ru_nivcsw','ru_exutime'])
        
        self.add('mailUser',line.pop(0),False)
        self.add('projectname',line.pop(0),blank=True)
        self.add('exitStatus',int(line.pop(0)),False)
        
        self.add('maxNumProcessors',line.pop(0))
        
        self.add('loginShell',line.pop(0),False)
        
        assert self.add('timeEvent',line.pop(0),False)==''
        self.add('idx',int(line.pop(0)))

        self.add('maxRMem',int(line.pop(0)))
        self.add('maxRSwap',int(line.pop(0)))

        assert self.add('inFileSpool',line.pop(0),False)==''
        assert self.add('commandSpool',line.pop(0),False)==''
        assert self.add('rsvId',line.pop(0),False)==''
        assert self.add('sla',line.pop(0),False)==''
        
        self.add('exceptMask',int(line.pop(0))) ## Bitmask

        ## Skip the next 15 entries
        line=line[15-1:-1]
        
        assert self.add('jobDescription',line.pop(0),False)==''
        
        ## Skip the next 4 entries
        line=line[4-1:-1]
        
        assert len(line)==0
        
        #print("job.extract> Unknown %d:%s" % (self.number,line))

        ## Hack to only generate metadata once.
        global first
        if first:
            first=False


def write(jobs):
    writer=csv.writer(out,delimiter="\t",quoting=csv.QUOTE_MINIMAL)
    header=list(meta)
    #header.sort()
    writer.writerow(header)
    try:
        for j in jobs:
            output=[]
            for k in header:
                v=j.tag.get(k,'')
                output.append(v)
            #print("##",output)
            writer.writerow(output)
    except Exception as e:
        import traceback
        print("lsfconvert>",output)
        print(e)
        traceback.print_exc()
        exit()

def display(jobs):
    header=list(meta)
    #header.sort()
    print(header)
    for k in header:
        print("-------- %s ---------" % k)
        for j in jobs:
            v=j.tag.get(k,None)
            if v:
                print(v)
       
def main(data):
    print("lsfconvert>")
    count=0
    reader=csv.reader(data,delimiter=' ',quotechar='"')
    for line in reader:
        display=list(line)
        try:
            count+=1
            j=Job()
            if limit and count>=limit:
                break
            j.extract(line)
            jobs.append(j)
        except Exception as e:
            import traceback
            print("lsfconvert>",display)
            print("lsfconvert>",line)
            print(e)
            traceback.print_exc()
            exit()
    print("lsfconvert>", count)


if __name__=='__main__':
    main(data)
    print(meta)
    #display(jobs)
    write(jobs)
    print("lsfconvert> done")

