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
 - library(plotrix)
 - library(MASS)
 - library(lubridate)
 - library(zoo)
 - library(rgeos)
 - library(stringi)
 - library(parallel)
 - library(gdata)

To install a package on R you just type the command
    
    install.packages('package_name')
    
# Running the script

The solution is composed by three parts, getting the data from cassandra, instantiating values to each campaign and processing the data to create a model and generate the heatmap.

In order to collect the data from cassandra it is needed to execute de scala programam encapsulated in the JAR file get-heat-map-data_2.11-1.0.jar. In order to do this one must run the following code on terminal:

    spark-submit --master local[*] get-heat-map-data_2.11-1.0.jar option1 option2 option3 option4

all parameters are mandatory

 - __option1__: A string to choose a client
 - __option2__: An integer to choose the campaign
 - __option3__: A string to choose the inital datetime to begin to extract the data
 - __option4__: A string to choose the final datetime to end the data extraction

So an example would be like the call beneath, which will collect the data from pernambucanas, campaign 2 and only the 14th day of november since the day starts at 00:00:00 and ends at 23:59:59

spark-submit --master local[*] get-heat-map-data_2.11-1.0.jar "pernambucanas" 2 "2017-11-14 00:00:00" "2017-11-15 00:00:00"

The data will be save as a text file in a structure such as _/home/centos/heatmap_auto/client_campaign/dados/initial-date_final-date/part-00000_ to be used on a R script 

To run the R script just use the following comand on terminal

    Rscript --vanilla heatmap_prod.R option1 option2 option3 option4

all parameters are mandatory and exactly the same as mentioned above, to get the data using scala.

This command will execute the heatmap_prod.R, which in turn will source the clients_info.R code that defines global variables of clients, and will keep on preparing the data and making the heatmaps for populational density and time spent, based on the models stored in the file _/home/centos/heatmap_auto/client_campaign/modelos/modelo.RData_, and saving the images as _/home/centos/heatmap_auto/client_campaign/mapas/date.png_ file.

The images will than be rotated and uploaded to a S3 bucket. All of this process are currently being done by the bash script script_daily.

The models is being made manually using the automated\_heatmap\_bothv02\_covmodels.R, which reads the dataset file in a specific folder collected by the scala case already mentioned. Than the script will make the model, unsig a frequentist approach, that will than be used to make the plot.

If no spatial dependency is observed than the code will use a old model instead of crashing. The code does this by looking at some predefined semivariograns. Based on the semi-variogram, a grid (100 x 100) of initial parameters are set and teste.

After that the script uses the best tested model, from a group of 42 different models, into the function krige.conv, responsable to interpolate the values in the specified grid of points (80 x 80).

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

