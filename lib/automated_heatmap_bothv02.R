args = commandArgs(trailingOnly=TRUE)

# Test if there is at least one argument: if not, return an error
if (length(args)==0) {
  stop("All the following arguments must be supplied:1. Must the code train a new model?\n\t2. Campaing\n\t3. Stablishment\n\t4. Period (in days) to train the newmodel", call.=FALSE)
}

# Preventing possible parameters entry errors
must_train = as.logical(to_upper(args[1]))
client = to_lower(args[2])
stablishment = as.integer(args[3])
period = as.integer(args[4])

if(class(must_train)!='logical'){
  stop('Argument 1 invalid.\nOptions are:\n\tTRUE\n\tFALSE',call. = FALSE)
}
if(!client%in%client_list){
  stop('Argument 3 invalid.\nClient Does not exist',call. = FALSE)
}
if(class(stablishment)!='integer'){
  stop('Argument 4 invalid.\nPass a valid integer',call. = FALSE)
}else if(!stablishment%in%stablishment_list){
  stop('Argument 4 invalid.\nStablishment does not exist',call. = FALSE)
}
if(class(period)!='integer'){
  stop('Argument 4 invalid.\nOptions are:\n\tAny integer number',call. = FALSE)
}

if(must_train == T){
  date_filter = c(today() - days(period),  today())
  # If hours is desired
  # now() - hours(period)
  # now() - weeks(period)
  # ...
}else{
  date_filter = c(today(), today())
}

################################################################################################
# APAGAR ESSAS LINHAS DE TESTE PARA NAO FICAR FIXO #
################################################################################################
must_train = T
client = 'claro'
stablishment = 1
period = 6
################################################################################################

# Preparing data to geoR format
source('/home/christopher/Documentos/Sense/Sense-Data-Science/heatmap_general/lib/prep_datav02.R')

# Create geodata object
geodata_density = list(coords = general.mean[,.(x,y)]/67.4, data = general.mean$signal, borders = eval(parse(text = paste0('coords$',client)))/67.4)
class(geodata_density) = 'geodata'
geodata_time = list(coords = general.mean[,.(x,y)]/67.4, data = general.mean$time, borders = eval(parse(text = paste0('coords$',client)))/67.4)
class(geodata_time) = 'geodata'

# Indentify outliers
# outlier_detec = function(geodata){
#   q1 = quantile(geodata$data,0.25)
#   q3 = quantile(geodata$data,0.75)
#   lims = q3 + 1.5*(q3-q1)
#   limi = q1 - 1.5*(q3-q1)
#   p = sum(geodata$data < limi | geodata$data > lims)/length(geodata$data)
#   if(p < 0.2){  
#     # Remove outliers
#     geodata$data = geodata$data[geodata$data <= lims]
#     geodata$coords = geodata$coords[geodata$data <= lims]
#     geodata$data = geodata$data[geodata$data >= limi]
#     geodata$coords = geodata$coords[geodata$data >= limi]
#   }
#   return(geodata)
# }

# geodata_density = outlier_detec(geodata_density)
# geodata_time = outlier_detec(geodata_time)

grid_length = c(30,30)

# Define grid to interpolate
gr0 = expand.grid(seq(min(geodata_density$borders[,1]),max(geodata_density$borders[,1]), len=grid_length[1]), 
                  seq(min(geodata_density$borders[,2]),max(geodata_density$borders[,2]), len=grid_length[2]))

# Crop using shape
gr = locations.inside(gr0, geodata_density$borders)

# Verify spatial dependency via semi-variograms
get_variog = function(geodata){
  variogram = list()
  variogram.env = list()
  
  variogram$v1 = tryCatch(variog(geodata), error = function(cond){return(NULL)})
  variogram.env$v1 = tryCatch(variog.mc.env(geodata, obj=variogram$v1), error = function(cond){return(NULL)})
  
  variogram$v2 = tryCatch(variog(geodata, fix.lam=F), error = function(cond){return(NULL)})
  variogram.env$v2 = tryCatch(variog.mc.env(geodata, obj=variogram$v2), error = function(cond){return(NULL)})
  
  variogram$v3 = tryCatch(variog(geodata, trend="1st"), error = function(cond){return(NULL)})
  variogram.env$v3 = tryCatch(variog.mc.env(geodata, obj=variogram$v3), error = function(cond){return(NULL)})
  
  variogram$v3a = tryCatch(variog(geodata, trend="1st", fix.lam=F), error = function(cond){return(NULL)})
  variogram.env$v3a = tryCatch(variog.mc.env(geodata, obj=variogram$v3a), error = function(cond){return(NULL)})
  
  variogram$v4 = tryCatch(variog(geodata, trend="2nd"), error = function(cond){return(NULL)})
  variogram.env$v4 = tryCatch(variog.mc.env(geodata, obj=variogram$v4), error = function(cond){return(NULL)})
  
  variogram$v4a = tryCatch(variog(geodata, trend="2nd", fix.lam=F), error = function(cond){return(NULL)})
  variogram.env$v4a = tryCatch(variog.mc.env(geodata, obj=variogram$v4a), error = function(cond){return(NULL)})
  
  variogram$v5 = tryCatch(variog(geodata,max.dist=(max(abs(geodata$coords[,1]))-min(abs(geodata$coords[,1])))/2), error = function(cond){return(NULL)})
  variogram.env$v5 = tryCatch(variog.mc.env(geodata, obj=variogram$v5), error = function(cond){return(NULL)})
  
  variogram$v5a = tryCatch(variog(geodata,fix.lam = F,max.dist=(max(abs(geodata$coords[,1]))-min(abs(geodata$coords[,1])))/2), error = function(cond){return(NULL)})
  variogram.env$v5a = tryCatch(variog.mc.env(geodata, obj=variogram$v5a), error = function(cond){return(NULL)})
  
  variogram$v5b = tryCatch(variog(geodata,trend = '1st',max.dist=(max(abs(geodata$coords[,1]))-min(abs(geodata$coords[,1])))/2), error = function(cond){return(NULL)})
  variogram.env$v5b = tryCatch(variog.mc.env(geodata, obj=variogram$v5b), error = function(cond){return(NULL)})
  
  variogram$v5c = tryCatch(variog(geodata,trend = '2nd',max.dist=(max(abs(geodata$coords[,1]))-min(abs(geodata$coords[,1])))/2), error = function(cond){return(NULL)})
  variogram.env$v5c = tryCatch(variog.mc.env(geodata, obj=variogram$v5c), error = function(cond){return(NULL)})
  
  variogram$v5d = tryCatch(variog(geodata,fix.lam=F,trend = '1st',max.dist=(max(abs(geodata$coords[,1]))-min(abs(geodata$coords[,1])))/2), error = function(cond){return(NULL)})
  variogram.env$v5d = tryCatch(variog.mc.env(geodata, obj=variogram$v5d), error = function(cond){return(NULL)})
  
  variogram$v5e = tryCatch(variog(geodata,fix.lam = F,trend = '2nd',max.dist=(max(abs(geodata$coords[,1]))-min(abs(geodata$coords[,1])))/2), error = function(cond){return(NULL)})
  variogram.env$v5e = tryCatch(variog.mc.env(geodata, obj=variogram$v5e), error = function(cond){return(NULL)})
  return(list(variogram = variogram, variogram.env = variogram.env))
}

modeling = function(dependency,variogram,geodata,type){
  # Choosed semi-variogram to define model initial parameters
  if(any(dependency)){
    semivariogram = eval(parse(text = paste0('variogram$',names(dependency[dependency][1]))))
    
    # Define initial parameters
    partial_still = seq(min(semivariogram$v),max(semivariogram$v),length.out = 10)
    range = seq(min(semivariogram$u),max(semivariogram$u),length.out = 10)
    initial = as.data.table(expand.grid(partial_still = partial_still,range = range))
    nugget = semivariogram$v[1]
    
    # List of models to test
    mod = list()
    
    mod$m1 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=F), error = function(cond){return(NULL)})
    mod$m2 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=F, trend='1st'), error = function(cond){return(NULL)})
    mod$m3 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=F, trend='2nd'), error = function(cond){return(NULL)})
    mod$m4 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=F, cov.model='sph'), error = function(cond){return(NULL)})
    mod$m5 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=F, trend='1st', cov.model='sph'), error = function(cond){return(NULL)})
    mod$m6 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=F, trend='2nd', cov.model='sph'), error = function(cond){return(NULL)})
    mod$m7 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=F, cov.model='mat'), error = function(cond){return(NULL)})
    mod$m8 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, cov.model='mat',kappa=0.5), error = function(cond){return(NULL)})
    mod$m9 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, cov.model='mat',kappa=1.5), error = function(cond){return(NULL)})
    mod$m10 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, cov.model='mat',kappa=2.5), error = function(cond){return(NULL)})
    mod$m11 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, cov.model='mat',kappa=3.5), error = function(cond){return(NULL)})
    mod$m12 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, cov.model='mat',kappa=4), error = function(cond){return(NULL)})
    mod$m13 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=F, trend='1st', cov.model='mat'), error = function(cond){return(NULL)})
    mod$m14 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, trend='1st', cov.model='mat',kappa=0.5), error = function(cond){return(NULL)})
    mod$m15 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, trend='1st', cov.model='mat',kappa=1.5), error = function(cond){return(NULL)})
    mod$m16 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, trend='1st', cov.model='mat',kappa=2.5), error = function(cond){return(NULL)})
    mod$m17 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, trend='1st', cov.model='mat',kappa=3.5), error = function(cond){return(NULL)})
    mod$m18 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, trend='1st', cov.model='mat',kappa=4), error = function(cond){return(NULL)})
    mod$m19 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=F, trend='2nd', cov.model='mat'), error = function(cond){return(NULL)})
    mod$m20 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, trend='2nd', cov.model='mat',kappa=0.5), error = function(cond){return(NULL)})
    mod$m21 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, trend='2nd', cov.model='mat',kappa=1.5), error = function(cond){return(NULL)})
    mod$m22 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, trend='2nd', cov.model='mat',kappa=2.5), error = function(cond){return(NULL)})
    mod$m23 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, trend='2nd', cov.model='mat',kappa=3.5), error = function(cond){return(NULL)})
    mod$m24 = tryCatch(likfit(geodata, ini=initial, fix.nug=F, fix.kappa=T, trend='2nd', cov.model='mat',kappa=4), error = function(cond){return(NULL)})
    mod$m25 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=F), error = function(cond){return(NULL)})
    mod$m26 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=F, trend='1st'), error = function(cond){return(NULL)})
    mod$m27 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=F, trend='2nd'), error = function(cond){return(NULL)})
    mod$m28 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=F, cov.model='sph'), error = function(cond){return(NULL)})
    mod$m29 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=F, trend='1st', cov.model='sph'), error = function(cond){return(NULL)})
    mod$m30 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=F, trend='2nd', cov.model='sph'), error = function(cond){return(NULL)})
    mod$m31 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=F, cov.model='mat'), error = function(cond){return(NULL)})
    mod$m32 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, cov.model='mat',kappa=0.5), error = function(cond){return(NULL)})
    mod$m33 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, cov.model='mat',kappa=1.5), error = function(cond){return(NULL)})
    mod$m34 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, cov.model='mat',kappa=2.5), error = function(cond){return(NULL)})
    mod$m35 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, cov.model='mat',kappa=3.5), error = function(cond){return(NULL)})
    mod$m36 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, cov.model='mat',kappa=4), error = function(cond){return(NULL)})
    mod$m37 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=F, trend='1st', cov.model='mat'), error = function(cond){return(NULL)})
    mod$m38 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, trend='1st', cov.model='mat',kappa=0.5), error = function(cond){return(NULL)})
    mod$m39 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, trend='1st', cov.model='mat',kappa=1.5), error = function(cond){return(NULL)})
    mod$m40 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, trend='1st', cov.model='mat',kappa=2.5), error = function(cond){return(NULL)})
    mod$m41 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, trend='1st', cov.model='mat',kappa=3.5), error = function(cond){return(NULL)})
    mod$m42 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, trend='1st', cov.model='mat',kappa=4), error = function(cond){return(NULL)})
    mod$m43 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=F, trend='2nd', cov.model='mat'), error = function(cond){return(NULL)})
    mod$m44 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, trend='2nd', cov.model='mat',kappa=0.5), error = function(cond){return(NULL)})
    mod$m45 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, trend='2nd', cov.model='mat',kappa=1.5), error = function(cond){return(NULL)})
    mod$m46 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, trend='2nd', cov.model='mat',kappa=2.5), error = function(cond){return(NULL)})
    mod$m47 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, trend='2nd', cov.model='mat',kappa=3.5), error = function(cond){return(NULL)})
    mod$m48 = tryCatch(likfit(geodata, ini=initial, fix.nug=T, nugget = nugget, fix.kappa=T, trend='2nd', cov.model='mat',kappa=4), error = function(cond){return(NULL)})
    
    print('mod.bic')
    # Choose best model using BIC
    mod.bic = sapply(mod,function(x)x$BIC)
    print('best_model_names')
    best_model_name = names(sort(mod.bic)[1])
    print('best_model')
    best_model = eval(parse(text = paste0('mod$',best_model_name)))
    # print('save')
    # save(best_model,file = paste0('../models_',type,'/model_',today(),'.RData'))
    print('kc')
    kc = krige.conv(geodata, loc=gr0,krige=krige.control(obj.model=best_model),output=output.control(n.predictive=500))
    return(list(kc = kc, model = best_model))
  }else{
    mod_files = list.files(paste0('../models_',type))
    last_model = sort(mod_files,decreasing = T)[1]
    load(paste0('../models_',type,'/',last_model))
    
    last_model = eval(parse(text = as.character(as.data.table(ll())[data.class == 'likGRF']$member)))
    kc = krige.conv(geodata, loc=gr0,krige=krige.control(obj.model=last_model),output=output.control(n.predictive=500))
    return(list(kc = kc, model = last_model))
  }
}

if(must_train == T){
  set.seed(9)
  # Verify spatial dependency via semi-variograms
  variogram_density = get_variog(geodata_density)$variogram
  variogram.env_density = get_variog(geodata_density)$variogram.env
  variogram_time = get_variog(geodata_time)$variogram
  variogram.env_time = get_variog(geodata_time)$variogram.env
  
  # Checks for spatial dependency on semi-variogram
  dependency_density = mapply(function(x,y){
    any(x$v < y$v.lower | x$v > y$v.upper)
  },variogram_density,variogram.env_density)
  
  dependency_time = mapply(function(x,y){
    any(x$v < y$v.lower | x$v > y$v.upper)
  },variogram_time,variogram.env_time)
  
  model_density = modeling(dependency_density,variogram_density,geodata_density,'density')
  kc_density = model_density$kc
  mod_density = model_density$model
  model_time = modeling(dependency_time,variogram_time,geodata_time,'time')
  kc_time = model_time$kc
  mod_time = model_time$model
}else{
  mod_files_density = list.files('../models_density')
  last_model_density = sort(mod_files_density,decreasing = T)[1]
  load(paste0('../models_density/',last_model_density))
  # load('/home/christopher/Documentos/Sense/Sense-Data-Science/heatmap_general/models_signal/model_2017-05-01.RData')
  last_model_density = eval(parse(text = as.character(as.data.table(ll(),keep.rownames = T)[Class == 'likGRF']$rn)))
  kc_density = krige.conv(geodata_density, loc=gr0,krige=krige.control(obj.model=last_model_density),output=output.control(n.predictive=500))
  rm(list = c(as.character(as.data.table(ll())[data.class == 'likGRF']$member), 'last_model_density'))
  
  mod_files_time = list.files('../models_time')
  last_model_time = sort(mod_files_time,decreasing = T)[1]
  load(paste0('../models_time/',last_model_time))
  
  last_model_time = eval(parse(text = as.character(as.data.table(ll(),keep.rownames = T)[Class == 'likGRF']$rn)))
  kc_time = krige.conv(geodata_time, loc=gr0,krige=krige.control(obj.model=last_model_time),output=output.control(n.predictive=500))
  rm(list = c(as.character(as.data.table(ll())[data.class == 'likGRF']$member), 'last_model_time'))
}

############################################################################################
#                                             Heatmap Plot
############################################################################################

png(paste0('../output/KrigDensity',today,'.png'),units = 'px', width = 1000, height = 1000,res=96)
par(mar = rep(0, 4))
image(kc_density,col=my.color2(100),xlab = '',ylab='',main='',axes=T,ylim=c(-5,20))
color.legend(xl = col_legend$pernambucanas$xl, xr = col_legend$pernambucanas$xr, yb = col_legend$pernambucanas$yb, yt = col_legend$pernambucanas$yt, gradient =  'x', rect.col = my.color2(100), legend = round(seq(min(kc_density$predict),max(kc_density$predict),length.out = 5),2),cex = 1, srt=-90,pos = 3,offset=1.5)

plot(eval(parse(text = paste0('area_sp$',name_campaign))), add = T)
with(eval(parse(text = paste0('labels$',name_campaign))),text(x, y, de_para, cex = 1,srt=-90,family = 'Roboto'))
with(eval(parse(text = paste0('labels$',name_campaign))),text(leg.x, leg.y, text, cex = 1,srt=-90,family = 'Roboto',adj=0))
dev.off()

png(paste0('../output/KrigTime',today,'.png'),units = 'px', width = 1000, height = 1000,res=96)
par(mar = rep(0, 4))
image(kc_time,col=my.color2(100),xlab = '',ylab='',main='',axes=T,ylim=c(-5,20))
color.legend(xl = col_legend$pernambucanas$xl, xr = col_legend$pernambucanas$xr, yb = col_legend$pernambucanas$yb, yt = col_legend$pernambucanas$yt, gradient =  'x', rect.col = my.color2(100), legend = round(seq(min(kc_time$predict),max(kc_time$predict),length.out = 5),2),cex = 1, srt=-90,pos = 3,offset=1.5)

plot(eval(parse(text = paste0('area_sp$',name_campaign))), add = T)
with(eval(parse(text = paste0('labels$',name_campaign))),text(x, y, de_para, cex = 1,srt=-90,family = 'Roboto'))
with(eval(parse(text = paste0('labels$',name_campaign))),text(leg.x, leg.y, text, cex = 1,srt=-90,family = 'Roboto',adj=0))
dev.off()
