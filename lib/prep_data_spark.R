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
library(parallel)
library(gdata)
###################################################

setwd('/home/christopher/Documents/Sense/Sense-Data-Science/heatmap_general/lib/')

source('./clients_info.R')

print('Preparing data ...')

# Define graphical variables
my.color = colorRampPalette(c("green", 'yellow',"red"))
my.color2 = colorRampPalette(c('cyan','green', 'yellow','orange',"red"))

# data = fread('/home/christopher/Downloads/claro1508_2908.txt',sep=',',na.strings = '')
data = fread('/home/christopher/Documents/Sense/HeatMaps/claro/dados/claro2309_2309.txt',sep=',',na.strings = '')
names(data) = c("id_sensor","rssi", "date_time", "id_campaign", "mac_address", "vendor","date_diaria","visit")

# Spark timestamp has a fixed .0 milisecond that is being removed
data[,date_time := as.POSIXct(stri_replace_all_fixed(date_time,'.0',''))]
data[,date_diaria := as.Date(date_diaria,format = '%Y-%m-%e')]
gc()

# RSSI mean
data = data[,.(rssi = mean(rssi)),by=.(id_sensor,mac_address,date_time,date_diaria,visit)]

# Date Diff in second
setorder(data,date_time)
data[,`:=`(data.max = max(date_time), data.min = min(date_time)),by=.(visit,mac_address,date_diaria)][,time.diff:=data.max-data.min]

# Mean signal by sensor
general.mean = data[,.(signal = mean(as.numeric(rssi),na.rm=T)),by=id_sensor]
# Mean time(in minutes) by sensor. Time will be counted only if the signal is strong enough, greater than or igual to -65
general.mean.time = data[rssi>=median(data$rssi),.(count = length(unique(mac_address)),time = mean(as.numeric(time.diff)/60, na.rm = T)),by=id_sensor]
# Merge info
general.mean = merge(general.mean, general.mean.time, all.x=T, by = 'id_sensor')

# Mean signal by sensor and date
data.per.sensor = data[,.(signal = mean(as.numeric(rssi), na.rm=T)),by=.(id_sensor,date_diaria)]
# Mean time by sensor and date
data.per.sensor.time = data[rssi>=median(data$rssi),.(count = length(unique(mac_address)),time = mean(as.numeric(time.diff)/60, na.rm = T)),by=.(id_sensor,date_diaria)]
# Merge info
data.per.sensor = merge(data.per.sensor, data.per.sensor.time, all.x=T, by = c('id_sensor','date_diaria'))

client = 'claro'
# Final dataset
general.mean = merge(general.mean,eval(parse(text = paste0('macs$',client,'[,.(id_sensor,x,y)]'))),all.x = T,by='id_sensor')
general.mean = general.mean[!is.na(x)]
general.mean[,`:=`(x=as.numeric(x),y=as.numeric(y))]
data.per.sensor = merge(data.per.sensor,eval(parse(text = paste0('macs$',client,'[,.(id_sensor,x,y)]'))),all.x = T,by='id_sensor')
data.per.sensor = data.per.sensor[!is.na(x)]
data.per.sensor[,`:=`(x=as.numeric(x),y=as.numeric(y))]
