 ## R analysis for lsf-stats Copyright 2014 Timothy Middelkoop and Tyler D. Stratton License Apache 2.0
 
 ## Read data
 setwd("C:/Users/tstratton/Documents/Github")
 
 d <- read.csv('lsf-stats/lewis-log.csv',sep="\t", header=TRUE)
 n <- nrow(d) ; n
 names(d)
 
 summary(d$CPU_T)
 max(d$CPU_T)

 onecore <- subset(d,d$cores==1)
 summary(d$cores)
normal <- subset(d, d$Queue=="normal")
 normal[0:100,]

