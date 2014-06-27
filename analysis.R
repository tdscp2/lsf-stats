 ## R analysis for lsf-stats Copyright 2014 Timothy Middelkoop and Tyler D. Stratton License Apache 2.0
 
 ## Read data
 d <- read.csv('lsf-stats/lewis-log.csv',sep="\t", header=TRUE);
 n <- nrow(d) ; n
 names(d)
 
 levels(d$Queue)
 