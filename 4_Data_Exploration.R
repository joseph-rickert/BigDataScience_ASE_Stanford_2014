#####################################################
#####################################################
##
## RevoScaleR Class 
## 
## Data Exploration
## Stanford ASE Big Data Conference 5/31/14
#####################################################
#####################################################

## RevoScaleR ships out with sample data, the location of which is one of the rxOptions parameters

?rxOptions				# for setting global options for Revolution’s global environment
rxOptions()				# a list
sampleDataDir <- rxOptions()[["sampleDataDir"]]
						# index into the list

dir(sampleDataDir)		# Look at the contents

## getwd()				# Look at you default working directory


## Section 1
## Let's work with the Dow Jones data file
DJIAdata <- file.path(sampleDataDir, "DJIAdaily.xdf") 
rxGetInfo(DJIAdata)		# default parameters, obtain basic info

## Section 2
rxGetVarInfo(DJIAdata)	# equivalent... obtain variable info
rxGetInfo(DJIAdata,getVarInfo=TRUE)
rxGetInfo(DJIAdata,getVarInfo=TRUE,numRows=5)

## Section 3
## Let's look at the structure of the rxGetVarInfo object
varInfoObject <- rxGetVarInfo(DJIAdata)
class(varInfoObject)
str(varInfoObject)

names(varInfoObject)
class(varInfoObject[[1]])
varInfoObject[["Open"]]
class(varInfoObject[["Open"]])
names(varInfoObject[["Open"]])
varInfoObject[["Open"]][["storage"]]
names(varInfoObject[["DayOfWeek"]])
varInfoObject[["DayOfWeek"]][["levels"]]

## Section 4
## a bit more info about variables: summary
rxSummary(~ Open, data = DJIAdata)
						# summary of a single variable

rxQuantile(varName = "Open", data = DJIAdata) 
						# if you want estimates of the quantiles

rxSummary(~Open+High+Low+Close, data=DJIAdata)
rxSummary(~., DJIAdata)	# summary of all the variables

## Section 5
## we can calculate weighted averages, based on volume of trade

?rxSummary  # options for both probability weights and frequency weights
rxSummary(~Open+High+Low+Close, data=DJIAdata, fweights="Volume")

## for factor variables, we have crosstabs:
rxCrossTabs(~DayOfWeek, data = DJIAdata)
rxCrossTabs(~DayOfWeek, data = DJIAdata, fweights="Volume")

## Section 6
## And a bit of visualization:

rxHistogram(~Open, data = DJIAdata,xNumTicks=10)				# basic histogram
rxHistogram(~DayOfWeek, data = DJIAdata)						# bar chart for factor data
rxHistogram(~Open | DayOfWeek, data = DJIAdata,xNumTicks=10)    # Faceting
rxHistogram(~Open, data = DJIAdata, fweights="Volume",xNumTicks=10)

rxLinePlot(Open~DaysSince1928, data = DJIAdata)
rxLinePlot(log(Open)~DaysSince1928, data = DJIAdata)

 
## Section 7
## Let's look at investigate rxCrossTabs a little further 
##

censusWorkers <- file.path(sampleDataDir, "CensusWorkers.xdf")
rxGetInfo(censusWorkers,getVarInfo=TRUE)

## creates cross-tabulations of categorical variables
## can have a dependent variable
## can have on-the fly transformation: F(age)

censusCTabs <- rxCrossTabs(wkswork1 ~ F(age) : sex, data = censusWorkers,pweights = "perwt")
censusCTabs

## by default shows sums. We can look at means, too

censusCTabs <- rxCrossTabs(wkswork1 ~ F(age) : sex, data = censusWorkers,pweights = "perwt", means=TRUE)
censusCTabs

## rxCube is an alternative to rxCrossTabs:
## Same output as rxCrossTabs, except data is returned in 'long' format

cube <- rxCube(wkswork1 ~ F(age) : sex, data = censusWorkers, pweights = "perwt", means=TRUE)
cube 

cubeDF <- as.data.frame(cube)
head(cubeDF)

#####################################################
#####################################################
##
## Hands-on exercise: export an rxCrossTabs/rxCube 
## result to a DF and plot using ggplot2 or lattice 
## (or other open source function)
##
#####################################################
#####################################################

censusWorkers <- file.path(sampleDataDir, "CensusWorkers.xdf")
rxGetInfo(censusWorkers,getVarInfo=TRUE)

cube <- rxCube(wkswork1 ~ F(age) : sex, data = censusWorkers, pweights = "perwt", means=TRUE)
cube 

cubeDF <- as.data.frame(cube)
head(cubeDF)

install.packages("ggplot2")
require(ggplot2)

p <- ggplot(cubeDF, aes(age, wkswork1))
p + geom_point(aes(colour = sex))









