#!/usr/bin/python
## lsf-convert Copyright 2014 Timothy Middelkoop License Apache 2.0

data='data/lewis-log.txt'

import re

### Example data
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


jobs={}

class Job:
    number=None
    tag={}
    events=[]
    
    def __repr__(self):
        return "<%d: %s" % (self.number,str(self.tag))

    def extract(self,line):
        #print "##",line
        if line=='':
            pass
        elif re.match('^Job',line):
            attributes=line.split(', ')
            for a in attributes:
                #print "??", a
                match=re.match('^(.+)\s+\<(.+)\>$',a)
                if match:
                    _tag,_value=match.groups()
                else:
                    self.tag[a]=True
                self.tag[_tag]=_value
                #print "??",_tag,_value
            self.number=int(self.tag['Job'])
            jobs[self.number]=self
        elif re.match('^(.+):\s+(.+)[\.\;]$',line):
            date,data=re.match('^(.+):\s+(.+)[\.\;]$',line).groups() ## yes duplicates
            self.event(date,data)
        else:
            print "job.extract> Unknown",line
            
    def event(self,date,data):
        if re.match('^Completed',data):
            if data=='Completed <done>':
                self.tag['Completed']=True
            else:
                self.tag['Completed']=False
        else:
            print "job.event>",date,data
            
    def summary(self,header,values):
        header=header.split()
        values=values.split()
        for h,v in zip(header,values):
            self.tag[h]=v

        
def main(data):
    print "lsfconvert>"
    count=0
    job=None
    summary=0
    for line in lines(data):
        #print "!!",line
        if re.match('^---',line):
            job=Job()
            summary=0
            count+=1
            line=None
            if count>100:
                return
        elif re.match('^Accounting',line):
            summary=1
        elif summary:
            summary+=1
            if summary==2:
                header=line
            elif summary==3 and job:
                job.summary(header,line)
        elif job:
            job.extract(line)

    print count


## Custom line iterator to remove EOL and wrapped
def lines(data):
    line=None
    for l in open(data).xreadlines():
        l=l[0:-1]
        if re.match('^                     ',l):
            line+=l[21:]
        elif line is None:
            line=l
        else:
            yield line
            line=l


if __name__=='__main__':
    main(data)
    for j in jobs.values():
        print j


