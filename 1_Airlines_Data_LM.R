
#####################################################
#####################################################
##
## RevoScaleR Big Airlines Demo
## Modified 8/9/13 by JB Rickert for Wharton Class
## Stanford ASE Big Data Conference 5/31/14
##
## Data sets required: 
## Airlines_87_08 - This is the large airlines dataset
##                - source: http://www.revolutionanalytics.com/subscriptions/datasets
##
#####################################################


# Load the Airlines Data

dataDir <- "C:/DATA/Airlines_87_08"
working.file <- file.path(dataDir,"BigAir3")

# Look at the meta-data
rxGetInfoXdf(working.file,getVarInfo=TRUE)
#
rxSummary(~ArrDelay,data=working.file)
rxSummary(~ArrDelay + DepDelay,data=working.file)
###############################################################################
# Code for linear model
###############################################################################
system.time(ArrDelayLM <- rxLinMod(ArrDelay ~ DayOfWeek:F(CRSDepTime),data=working.file, cube=TRUE, blocksPerRead=80))
#


summary(ArrDelayLM)
#Plot the coefficients
ADDT <- ArrDelayLM$countDF
names(ADDT) <- c("DayOfWeek","DepartureHour","ArrDelay","Counts")
ADDT$DepartureHour <- as.integer(as.character(ADDT$DepartureHour))
rxLinePlot(ArrDelay ~ DepartureHour | DayOfWeek, data=ADDT,main="Average Arrival Delay by Day of Week by Departure Hour")
#



# Coefficients and same as doing a cube
system.time(ADcube <- rxCube(ArrDelay ~ DayOfWeek:F(CRSDepTime), 
    data=working.file, returnDataFrame=TRUE, blocksPerRead=5 )) 
ADcube[1:10,]
#> ADcube[1:10,]
   #DayOfWeek F_CRSDepTime ArrDelay Counts
#1     Monday            0 6.687033 142344
#2    Tuesday            0 6.318279 129261
#3  Wednesday            0 7.891790 128777
#4   Thursday            0 8.692393 125657
#5     Friday            0 8.381638 126683
#6   Saturday            0 5.476547 119302
#7     Sunday            0 7.217032 128032
#8     Monday            1 2.447749  31033
#9    Tuesday            1 1.419292  30406
#10 Wednesday            1 1.893710  30304
summary(ArrDelayLM)
