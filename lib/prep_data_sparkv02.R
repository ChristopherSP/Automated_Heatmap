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

setwd('/home/christopher/Documentos/Sense/Sense-Data-Science/heatmap_general/lib/')

source('./clients_info.R')

print('Preparing data ...')

# Define graphical variables
my.color = colorRampPalette(c("green", 'yellow',"red"))
my.color2 = colorRampPalette(c('cyan','green', 'yellow','orange',"red"))

general.mean = fread('/home/christopher/Documents/Sense/HeatMaps/claro/dados/general_mean_claro.txt',sep=',')
names(general.mean) = c()
data.per.sensor = fread('/home/christopher/Documents/Sense/HeatMaps/claro/dados/data_per_sensor_claro.txt',sep=',')
names(data.per.sensor) = c()

client = 'claro'
# Final dataset
general.mean = merge(general.mean,eval(parse(text = paste0('macs$',client,'[,.(id_sensor,x,y)]'))),all.x = T,by='id_sensor')
general.mean = general.mean[!is.na(x)]
general.mean[,`:=`(x=as.numeric(x),y=as.numeric(y))]
data.per.sensor = merge(data.per.sensor,eval(parse(text = paste0('macs$',client,'[,.(id_sensor,x,y)]'))),all.x = T,by='id_sensor')
data.per.sensor = data.per.sensor[!is.na(x)]
data.per.sensor[,`:=`(x=as.numeric(x),y=as.numeric(y))]
