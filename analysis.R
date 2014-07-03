## R analysis for lsf-stats Copyright 2014 Timothy Middelkoop and Tyler D. Stratton License Apache 2.0

######
## Load Data

## Read data
d <- read.csv('lsf-stats/lewis-log.csv',sep="\t", header=TRUE);
n <- nrow(d) ; n
names(d)

## Dates are always fun
d$time <- strptime(d$time,format="%F %H:%M:%S")
d$submit <- strptime(d$submit,format="%F %H:%M:%S")
d$begin <- strptime(d$begin,format="%F %H:%M:%S")
d$term <- strptime(d$term,format="%F %H:%M:%S")
d$start <- strptime(d$start,format="%F %H:%M:%S")

######
## Analysis

levels(d$Queue)
