
# Libraries
###################################################
library(data.table)
library(geoR)
library(shapefiles)
library(maptools)
library(sp)
library(ggplot2)
library(plotrix)
library(rgdal)
library(MASS)
library(tseries)
library(lubridate)
library(zoo)
library(rgeos)
library(stringi)
library(svDialogs)
library(RJDBC)
library(parallel)
###################################################

setwd('/home/christopher/Documentos/Sense/Sense-Data-Science/heatmap_general/lib/')

print('Getting clients info')
source('./clients_info.R')

print('Preparing data ...')

# Indentify outliers
outlier_iqr = function(rssi){
  q1 = quantile(rssi,0.25)
  q3 = quantile(rssi,0.75)
  lims = q3 + 1.5*(q3-q1)
  limi = q1 - 1.5*(q3-q1)
  idx = limi <= rssi & rssi <= lims
  return(idx)
}

outlier_percentile = function(rssi,perc=95){
  lims = 1 - ((100 - perc)/100)/2
  limi = ((100 - perc)/100)/2
  q0 = quantile(rssi,limi)
  q5 = quantile(rssi,lims)
  idx = q0<= rssi & rssi <= q5
  return(idx)
}

# Define graphical variables
my.color = colorRampPalette(c("green", 'yellow',"red"))
my.color2 = colorRampPalette(c('cyan','green', 'yellow','orange',"red"))

# Path to Cassandra's JAR
drv = JDBC("org.apache.cassandra.cql.jdbc.CassandraDriver",list.files("/home/christopher/Documentos/Sense/Sense-Data-Science/heatmap_general/etc/",pattern="jar$",full.names=T))
# Open connection with cassandra
connection = dbConnect(drv, "jdbc:cassandra://34.196.59.158:9160")

# Client list to check parameter
client_list = unique(dbGetQuery(connection,'SELECT company from zubat.sensor')$company)

# Stablishment list to check parameter
stablishment_list = unique(dbGetQuery(connection,paste0('SELECT id_campaign from ', client, '.sensor'))$id_campaign)

# Sensors coords
macs = as.data.table(dbGetQuery(connection,paste0('SELECT id_sensor, latitude as y , longitude as x, name from ',client,'.sensor where id_campaign = ',stablishment, ' ALLOW FILTERING')))
# Sensors list
macs_seq = unique(macs$id_sensor)

# Get data seq by hour
date_seq = seq(as.POSIXlt(paste0(date_filter[1],' 00:00:00')), as.POSIXlt(paste0(date_filter[2],' 00:00:00')), by='1 hour')
date_seq = data.table(date_ini = date_seq)
date_seq[,date_final := shift(date_ini,1,type='lead')]
date_seq = date_seq[!is.na(date_final)]
date_seq[,`:=`(date_ini = format(date_ini,'%Y-%m-%d %X'), date_final = format(date_final,'%Y-%m-%d %X'))]

# Iteraction date and id_sensors to iterate on mapply
iterator = expand.grid(i = 1:nrow(date_seq), j = macs_seq[1:3])
it_i = iterator$i
it_j = iterator$j

aux = mapply(function(i,j) dbGetQuery(connection,paste0('select id_sensor, mac_address, UnixTimestampOf(maxtimeuuid(date_time)) as date_time, cast(rssi as int) as rssi from ', client, ".measurement where id_sensor ='",j, "' ", "and date_time >= '", date_seq[i]$date_ini,"' and date_time < '", date_seq[i]$date_final,"'")),it_i,it_j, SIMPLIFY = F)

# Bind values
data = rbindlist(aux[!sapply(lapply(aux,FUN = function(x) if(nrow(x)>0)x),is.null)])

rm(list = 'aux')
# Cassandra timestamp is in milisecond while R uses seconds hence the division by 1000
data[,date_time := as.POSIXct(date_time/1000, origin="1970-01-01")]

# Adjusting date
data[,date2:=as.Date(date_time, format = '%Y-%m-%d %X')]

# RSSI mean
data = data[,.(rssi = mean(rssi)),by=.(id_sensor,mac_address,date,date2)]

# Date Diff in minutes
setorder(data,date)
data[,ds:=shift(date,1),by=.(id_sensor,mac_address,date2)]
data[,interval:=date-ds][,ds:=NULL]

# Counting visits
setkey(data,id_sensor,mac_address,date)
data[interval>=2*60, visit := 2:.N, by = .(id_sensor,mac_address,day(date))]
data[,visit:=na.locf(visit,fromLast = F,na.rm=F),by=.(id_sensor,mac_address,day(date))]
data[is.na(visit),visit:=1]

data[,`:=`(data.max = max(date), data.min = min(date)),by=.(id_sensor,visit,mac_address,date2)][,time.diff:=data.max-data.min]

# Check to see if everything is ok
# data[id_sensor == '98:DE:D0:DB:CB:18' & mac_address == 'F8:CF:C5:1C:31:5A']


##################################################################################################################
#                                                     Filters                                                    #
##################################################################################################################
# Computers, routers, ...
# 5 hours or more
devices = unique(data[time.diff >= 5*60*60]$mac_address)
# Employees
# More than 5 visits in a day. Visits that last more than 2 hours in more than 3 days. Visits before 10 a.m.
employees = unique(data[visit>=5 | (length(unique(date2))>3 & time.diff >= 2*60*60) | hour(date) <= 10]$mac_address)
# Viewers
# Macs recognized by at most 5 sensors
viewers = unique(data[,.(q = length(unique(id_sensor))),by=mac_address][q<=5]$mac_address)

# Filter
data = data[(!mac_address%in%c(devices, employees, viewers))]

# Remove outlier
out_idx = outlier_detec(data$rssi)
data = data[out_idx]

# Mean signal by sensor
general.mean = data[,.(signal = mean(as.numeric(rssi),na.rm=T)),by=id_sensor]
# Mean time(in minutes) by sensor. Time will be counted only if the signal is strong enough, greater than or igual to -65
general.mean.time = data[rssi>=median(data$rssi),.(count = length(unique(mac_address)),time = mean(as.numeric(time.diff)/60, na.rm = T)),by=id_sensor]
# Merge info
general.mean = merge(general.mean, general.mean.time, all.x=T, by = 'id_sensor')

# Mean signal by sensor and date
data.per.sensor = data[,.(signal = mean(as.numeric(rssi), na.rm=T)),by=.(id_sensor,date2)]
# Mean time by sensor and date
data.per.sensor.time = data[rssi>=median(data$rssi),.(count = length(unique(mac_address)),time = mean(as.numeric(time.diff)/60, na.rm = T)),by=.(id_sensor,date2)]
# Merge info
data.per.sensor = merge(data.per.sensor, data.per.sensor.time, all.x=T, by = c('id_sensor','date2'))

# # Sensors coordinates
# sense.pos.dt = as.data.table(do.call(rbind,macs))
# sense.pos.dt[,`:=`(x=as.numeric(x), y=as.numeric(y))]

# Final dataset
general.mean = merge(general.mean,eval(parse(text = paste0('macs$',client,'[,.(id_sensor,x,y)]'))),all.x = T,by='id_sensor')
general.mean = general.mean[!is.na(x)]
data.per.sensor = merge(data.per.sensor,eval(parse(text = paste0('macs$',client,'[,.(id_sensor,x,y)]'))),all.x = T,by='id_sensor')
data.per.sensor = data.per.sensor[!is.na(x)]
