#!/usr/bin/python
## lsf-convert Copyright 2014 Timothy Middelkoop License Apache 2.0

data=open('lewis-log.txt')
csv=open('lewis-log.csv','w')
limit=False
#limit=100000

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


jobs=[]
meta=set()

class Job:
    number=None
    task=None
    tag=None
    events=None
    
    def __init__(self):
        self.tag={}
        self.events=[]
    
    def __repr__(self):
        return "<%d: %s" % (self.number,str(self.tag))
        

    def extract(self,line):
        #print "##",line
        if line=='':
            pass
        ## Match Job
        elif re.match('^Job',line):
            attributes=line.split(', ')
            for a in attributes:
                #print "??", a
                ## Match tag <>
                match=re.match('^([\w\s]+) \<(.+)\>$',a)
                if match:
                    _tag,_value=match.groups()
                else:
                    self.tag[a]=True
                self.tag[_tag]=_value
                meta.add(_tag)
                #print "??",_tag,_value
            m=re.match('(\d+)(\[(\d+)\])?',self.tag['Job'])
            self.number=int(m.group(1))
            if m.group(2):
                self.task=int(m.group(3))
            jobs.append(self)
        ## Match Event date and data
        elif re.match('^(.+ \d+:\d+:\d+):\s+(.+)$',line):
            date,data=re.match('^(.+ \d+:\d+:\d+):\s+(.+)$',line).groups() ## yes duplicates
            self.event(date,data)
        else:
            print "job.extract> Unknown",line
            
    def event(self,date,data):
        #print "??", date
        if re.match('^Submitted from host',data):
            pass
        elif re.match('^Completed',data):
            meta.add('Completed')
            if data=='Completed <done>.':
                self.tag['Completed']=True
            else:
                self.tag['Completed']=False
        elif re.match('^Dispatched',data):
            meta.add('cores')
            cores=1
            m=re.match('^Dispatched to (\d+)',data)
            if m:
                cores=m.group(1)
            self.tag['cores']=cores
        elif re.search('dispatched to',data):
            meta.add('cores')
            cores=1
            m=re.match('\[\d+\] dispatched to (\d+)',data)
            if m:
                cores=m.group(1)
            self.tag['cores']=cores
        else:
            print "job.event> |%s|%s|" % (date,data)
            
    def summary(self,header,values):
        header=header.split()
        values=values.split()
        for h,v in zip(header,values):
            meta.add(h)
            self.tag[h]=v

        
def main(data):
    #print "lsfconvert>"
    count=0
    job=None
    summary=0
    for line in lines(data):
        try:
            #print "!!",line
            if re.match('^---',line):
                job=Job()
                summary=0
                count+=1
                line=None
                if limit and count>=limit:
                    break
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
        except Exception as e:
            import traceback
            print "lsfconvert>",line
            print e
            traceback.print_exc()
            exit()
    print "lsfconvert>", count

def display(jobs):
    for j in jobs:
        print j

def write(jobs):
    num_format = re.compile(r'^\-?[0-9]+\.?[0-9]*\s*$')
    header=list(meta)
    #print ','.join(header)
    csv.write("\t".join(header) + "\n")
    for j in jobs:
        output=[]
        for h in header:
            v=j.tag.get(h)
            if v is None:
                output.append('')
            elif type(v) is type(True):
                output.append('TRUE' if v else 'FALSE')
            elif type(v) is type('') and num_format.match(v):
                output.append(str(float(v)))
            else:
                output.append(str(v))
        #print ','.join(output)
        csv.write("\t".join(output))
        csv.write("\n")

## Custom line iterator to remove EOL and wrapped
def lines(data):
    line=None
    for l in data.xreadlines():
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
    print meta
    write(jobs)
    print "lsfconvert> done"

