# Libraries
###################################################
library(data.table)
library(geoR)
library(shapefiles)
library(maptools)
library(sp)
library(plotrix)
library(MASS)
library(lubridate)
library(zoo)
library(rgeos)
library(stringi)
library(parallel)
library(gdata)
###################################################

args = commandArgs(trailingOnly=TRUE)
if (length(args)==0) {
  stop("All four arguments must be supplied:\n\t1. Client\n\t2. Campaign\n\t3. Initial date time\n\t4. Final date time ", call.=FALSE)
}

client = args[1]
campaign = args[2]
dateini = args[3]
dateend = args[4]

# setwd("~/Downloads/")
# source("/home/user/Documents/Semantix/Sense-Data-Science/heatmap_general/lib/clients_info.R")
setwd('/home/centos/heatmap_auto/')
source('./clients_info.R')

# Define graphical variables
my.color = colorRampPalette(c("green", 'yellow',"red"))
my.color2 = colorRampPalette(c('cyan','green', 'yellow','orange',"red"))

data = fread(paste0('./',client,"/",campaign,'/dados/',unlist(strsplit(dateini,' '))[1],'_',unlist(strsplit(dateend,' '))[1],'/part-00000'),sep=',',na.strings = '')
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

# Final dataset
general.mean = merge(general.mean,eval(parse(text = paste0('macs$',client,"$campaign",campaign,'[,.(id_sensor,x,y)]'))),all.x = T,by='id_sensor')
general.mean = general.mean[!is.na(x)]
general.mean[,`:=`(x=as.numeric(x),y=as.numeric(y))]
data.per.sensor = merge(data.per.sensor,eval(parse(text = paste0('macs$',client,"$campaign",campaign,'[,.(id_sensor,x,y)]'))),all.x = T,by='id_sensor')
data.per.sensor = data.per.sensor[!is.na(x)]
data.per.sensor[,`:=`(x=as.numeric(x),y=as.numeric(y))]

##################################################################################################################################################
load(paste0('./',client,"/",campaign,'/modelos/modelo.RData'))

dias = unique(data.per.sensor$date_diaria)

lapply(dias,function(dia){
  
  geodata_density = list(coords = data.per.sensor[date_diaria == dia][,.(x,y)],data = data.per.sensor[date_diaria == dia]$signal, borders = eval(parse(text = paste0('coords$',client,"$campaign",campaign))))
  class(geodata_density) = 'geodata'
  geodata_time = list(coords = data.per.sensor[date_diaria == dia][,.(x,y)], data = data.per.sensor[date_diaria == dia]$time, borders = eval(parse(text = paste0('coords$',client,"$campaign",campaign))))
  class(geodata_time) = 'geodata'
  
  grid_length = c(80,80)
  gr0 = expand.grid(seq(min(geodata_density$borders[,1]),max(geodata_density$borders[,1]), len=grid_length[1]), 
                    seq(min(geodata_density$borders[,2]),max(geodata_density$borders[,2]), len=grid_length[2]))
  gr <<- locations.inside(gr0, geodata_density$borders)
  
  print('######################################\n')
  print('Sinal')
  print('######################################\n')
  
  kc_density = tryCatch(krige.conv(geodata_density, loc=gr0,krige=krige.control(obj.model=model_density),output=output.control(n.predictive=500)), error = function(cond){return(krige.conv(geodata_density, loc=gr0,krige=krige.control(obj.model=model_density)))})
  
  print('######################################\n')
  print('Tempo')
  print('######################################\n')
  
  kc_time = tryCatch(krige.conv(geodata_time, loc=gr0,krige=krige.control(obj.model=model_time),output=output.control(n.predictive=500)), error = function(cond){return(krige.conv(geodata_time, loc=gr0,krige=krige.control(obj.model=model_time)))})
  ############################################################################################
  #                                             Heatmap Plot
  ############################################################################################
  
  png(paste0('./',client,"/",campaign,'/mapas/',dia,'.png'),units = 'px', width = 1000, height = 1000,res=96)
  par(mar = rep(0, 4))
  image(kc_density,col=my.color2(100),xlab = '',ylab='',main='',axes=T,ylim=range(gr$Var2)+c(-2.5,2.5))
  plot(eval(parse(text = paste0('area_sp$',client,"_campaign",campaign))), add = T)
  with(eval(parse(text = paste0('labels$',client,"_campaign",campaign))),text(x, y, de_para, cex = 1,srt=-90,family = 'Roboto'))
  with(eval(parse(text = paste0('labels$',client,"_campaign",campaign))),text(leg.x, leg.y, text, cex = 1,srt=-90,family = 'Roboto',adj=0))
  dev.off()
  
  png(paste0('./',client,"/",campaign,'/mapas/',gsub('-','_',dia),'.png'),units = 'px', width = 1000, height = 1000,res=96)
  par(mar = rep(0, 4))
  image(kc_time,col=my.color2(100),xlab = '',ylab='',main='',axes=T,ylim=range(gr$Var2)+c(-2.5,2.5))
  plot(eval(parse(text = paste0('area_sp$',client,"_campaign",campaign))), add = T)
  with(eval(parse(text = paste0('labels$',client,"_campaign",campaign))),text(x, y, de_para, cex = 1,srt=-90,family = 'Roboto'))
  with(eval(parse(text = paste0('labels$',client,"_campaign",campaign))),text(leg.x, leg.y, text, cex = 1,srt=-90,family = 'Roboto',adj=0))
  dev.off()
})
