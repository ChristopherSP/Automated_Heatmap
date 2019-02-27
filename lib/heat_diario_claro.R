load('/home/christopher/Documents/Sense/modelos_claro.RData')

dia = '2017-09-23'

client = 'claro'

geodata_density = list(coords = data.per.sensor[date_diaria == dia][,.(x,y)]/67.4, data = data.per.sensor[date_diaria == dia]$signal, borders = eval(parse(text = paste0('coords$',client)))/67.4)
class(geodata_density) = 'geodata'
geodata_time = list(coords = data.per.sensor[date_diaria == dia][,.(x,y)]/67.4, data = data.per.sensor[date_diaria == dia]$time, borders = eval(parse(text = paste0('coords$',client)))/67.4)
class(geodata_time) = 'geodata'

grid_length = c(80,80)
gr0 = expand.grid(seq(min(geodata_density$borders[,1]),max(geodata_density$borders[,1]), len=grid_length[1]), 
                  seq(min(geodata_density$borders[,2]),max(geodata_density$borders[,2]), len=grid_length[2]))
gr = locations.inside(gr0, geodata_density$borders)

kc_density = krige.conv(geodata_density, loc=gr0,krige=krige.control(obj.model=model_density$model$m19),output=output.control(n.predictive=500))

kc_time = krige.conv(geodata_time, loc=gr0,krige=krige.control(obj.model=model_time$model$m19),output=output.control(n.predictive=500))
############################################################################################
#                                             Heatmap Plot
############################################################################################

png(paste0('/home/christopher/Documents/Sense/HeatMaps/',client,'/mapas/',dia,'.png'),units = 'px', width = 1000, height = 1000,res=96)
par(mar = rep(0, 4))
image(kc_density,col=my.color2(100),xlab = '',ylab='',main='',axes=T,ylim=c(-2,18))
plot(eval(parse(text = paste0('area_sp$',client))), add = T)
with(eval(parse(text = paste0('labels$',client))),text(x, y, de_para, cex = 1,srt=-90,family = 'Roboto'))
with(eval(parse(text = paste0('labels$',client))),text(leg.x, leg.y, text, cex = 1,srt=-90,family = 'Roboto',adj=0))
dev.off()

png(paste0('/home/christopher/Documents/Sense/HeatMaps/',client,'/mapas/',gsub('-','_',dia),'.png'),units = 'px', width = 1000, height = 1000,res=96)
par(mar = rep(0, 4))
image(kc_time,col=my.color2(100),xlab = '',ylab='',main='',axes=T,ylim=c(-2,18))
plot(eval(parse(text = paste0('area_sp$',client))), add = T)
with(eval(parse(text = paste0('labels$',client))),text(x, y, de_para, cex = 1,srt=-90,family = 'Roboto'))
with(eval(parse(text = paste0('labels$',client))),text(leg.x, leg.y, text, cex = 1,srt=-90,family = 'Roboto',adj=0))
dev.off()
