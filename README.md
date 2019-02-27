%md
# Heat Map for both Permanence Time and Populational density

This script generates a heat map  for both permanence time and populational density for the Pernambucanas store by modelling (in a frequentist way) the space dependencie between sensors signal measurements and interpolates the values on a pre-defined regular grid of interest

# Prerequisites

To run this script it's needed to have R installed in the computer, which can be achieved by the following line of code on the Ubuntu terminal

    sudo apt-get install r-base
    
The following packages are also needed:

 - library(data.table)
 - library(geoR)
 - library(shapefiles)
 - library(maptools)
 - library(sp)
 - library(ggplot2)
 - library(plotrix)
 - library(rgdal)
 - library(MASS)
 - library(tseries)
 - library(lubridate)
 - library(zoo)
 - library(rgeos)

To install a package on R you just type the command
    
    install.packages('package_name')
    
# Running the script

The code is composed by two parts, prep\_data.R and automated\_heatmap.R.

To run the script just use the following comand on terminal

    Rscript --vanilla option1 option2 option3 option4

all parameters are mandatory

option1 sets whether to make a new model or to use an old one. The values to this parameter are:

 - TRUE 
 - FALSE

TRUE makes a new model and FALSE use an old one.

option2 sets the client for which to make the heat map

option3 sets the stablishment of the client defined above

option4 sets the period (in days) to train the newmodel starting from the actual sys.date

The automated\_heatmap\_bothv02\_covmodels.R will source the prep\_data\_spark.R, which reads the dataset file in a specific folder collected by the get\_data.scala scrip and makes the dataframe. Than the script will make the model, unsig a frequentist approach, that will than be used to make the plot.

If no spatial dependency is observed than the code will use a old model instead of crashing. The code does this by looking at some predefined semivariograns. Based on the semi-variogram, a grid (100 x 100) of initial parameters are set and teste.

After that the script use the best tested model into the function krige.conv, responsable to interpolate the values in the specified grid of points (80 x 80).

After all is set and done the script saves the heat map into a png file.

For more details see the comments in the code.

## Input layout
The layout of input data must be as follows (without the header):

|id_sensor|rssi|date_time|id_campaign|mac_address|vendor|date_diaria|visit|
|-|-|-|-|-|-|-|-|
|18:D6:C7:43:56:56|-71|2017-09-05 17:42:21.0|1|0E:64:A8:BC:73:A0| |2017-9-5|0|
|98:DE:D0:DB:C1:74|-73|2017-09-05 17:42:21.0|1|0E:64:A8:BC:73:A0| |2017-9-5|0|


## Output layout
The output of the script is the interpolated mean of macs permanence time and populational density.

