#####################################################
#####################################################
##
## RevoScaleR Class
## 
## Data Manipulation
##
#####################################################
#####################################################

## Section 1
## Import

## dataframe to XDF file

myData <- data.frame(
	x=rnorm(100), 
	y=rnorm(100)+4, 
	z = rep(c("sdff", "hgdd"), 50))
head(myData)

rxDataFrameToXdf(data=myData, outFile="hello.xdf",overwrite = TRUE)
rxGetInfo(data="hello.xdf", getVarInfo=TRUE)

## Single csv to an XDF file
## Let's import one of the mortgage default files into an XDF file. 

workDir <- getwd()                                               # define where we'll write to
sampleDataDir <- rxOptions()[["sampleDataDir"]]                  # define where we'll read from
dir(sampleDataDir)

mort2001 <- file.path(sampleDataDir, "mortDefaultSmall2001.csv") # define a data source
mort2001Xdf <- file.path(workDir, "mortDefaultSmall2001.xdf")    # define destination file

## after we've defined a data source, we need to import it into an xdf file. 
?rxImport
rxImport(inData=mort2001, outFile = mort2001Xdf, overwrite=TRUE) # import

rxGetInfo(mort2001Xdf)                                           # get basic info
rxGetInfo(mort2001Xdf, getVarInfo=TRUE,numRows=5)                # get variable info
rxGetVarInfo(mort2001Xdf)                                        # get variable info

## Multiple csv's to an XDF file
## Let's import the mortDefaultSmall200X.csv files from the sample data folder into a single xdf file

mortData <- file.path(workDir, "mortgages.xdf")

firstYear <- TRUE
myAppend="none"

for (iYear in 2000:2009) {

	## build file name
	csvBaseName <- paste("mortDefaultSmall", iYear, ".csv", sep="")
	csvFileName <- file.path(sampleDataDir, csvBaseName)
	

	## read file into xdf file
	rxImport(inData=csvFileName, outFile = mortData,append=myAppend, overwrite=firstYear)
	
	## after the first year we don't need to overwrite anymore, just need to append rows
	firstYear <- FALSE
	myAppend="rows"
		
}

rxGetInfo(mortData, getVarInfo=TRUE)
rxHistogram(~ccDebt, data=mortData,xNumTicks=10)
#-----------------------------------------------------------------------------
## Section 2
## Transforming and subsetting data

## The DataStep
?rxDataStep
 
## Simple transforms
## example: experiencedWorker is a worker with more than 5 years of employment
rxDataStep(inData=mortData, 
	       outFile=mortData, 
	       transforms = list(experiencedWorker = yearsEmploy>=5), 
	       append="cols", 
		   overwrite=TRUE)

rxGetInfo(data=mortData, getVarInfo=TRUE)
rxHistogram(~experiencedWorker , data=mortData)


## what would happen if we were writing outputs to new file?
## adding varsToKeep in this case saves time (reading fewer variables - faster!)
rxDataStep(inData=mortData,
 	       outFile="hello2.xdf", 
		   varsToKeep="yearsEmploy",
	       transforms = list(experiencedWorker = yearsEmploy>=5), 
	       append="none", 
		   overwrite=TRUE)

rxGetInfo(data="hello2.xdf", getVarInfo=TRUE)

## More elaborate transforms
## transform functions
## example: normalize a variable 

minCS <- rxGetVarInfo(mortData)$creditScore[[3]]
maxCS <- rxGetVarInfo(mortData)$creditScore[[4]]

## defining a transformFunction:
simplyNormalize <- function(dataList) {
	
	dataList[["normCreditScore"]] <- (dataList[["creditScore"]] - minCreditScore) / (maxCreditScore-minCreditScore)
	dataList[["moreThanHalf"]] <- dataList[["normCreditScore"]] > 0.5
	dataList[["newConst"]] <- rep(23, length.out=length(dataList[[1]]))
	return(dataList)

}

## Note that inside the transformFunction the data is a list, not a dataframe
## (you have to make sure that the length of all elements are the same)
## Another quirk: the transform function is being evaluated in a 'sterile' environment
## which means: you have to pass variables into the function using transformObjects

rxDataStep(inData=mortData, 
	       outFile=mortData, 
	       transformFunc = simplyNormalize, 
		   transformObjects = list(minCreditScore=minCS, maxCreditScore=maxCS),
	       append="cols", 
		   overwrite=TRUE)

rxGetInfo(data=mortData, getVarInfo=TRUE)
rxHistogram(~normCreditScore, data=mortData,xnNumTicks=15)
rxSummary(~normCreditScore, data=mortData)

## HOW THINGS CAN GO WRONG WITH CHUNKING
## The implication of seeing one chunk at a time: you don't see all the data...
## ... so can't use min(), max(), quartiles, etc. inside the transform function

## example trying to normalize within each chunk
wrongNormalization <- function(dataList) {
	dataList[["normCreditScore2"]] <- (dataList[["creditScore"]] - min(dataList[["creditScore"]])) / 
                                      (max(dataList[["creditScore"]], na.rm=TRUE)-min(dataList[["creditScore"]], na.rm=TRUE))
	dataList[["normDiff"]] <- dataList[["normCreditScore2"]] - dataList[["normCreditScore"]]
	return(dataList)

}

mortData2 <- file.path(workDir, "mortgagesTemp.xdf")
rxDataStep(inData=mortData, 
	       outFile=mortData2, 
	       transformFunc = wrongNormalization, 
		   overwrite=TRUE)

rxGetInfo(mortData2, getVarInfo=TRUE)
rxSummary(~normCreditScore + normCreditScore2, data=mortData2)     # wrong
rxSummary(~normCreditScore, data=mortData)                         # correct


rxHistogram(~normCreditScore2, data=mortData2,xnNumTicks=15)       # wrong
rxHistogram(~normCreditScore, data=mortData,xnNumTicks=15)         # correct

#########################################
##
## Hands-on Exercise:
## - create a new file including only people with credit scores above 700
## Write a transform function to recode their credit scores as a factor 
##   with ranges of 50 (<=750, 751-800, 801-850, ...) Use the cut function with
##   the breaks parameter set to come up with the levels, and create a new character variable,
##   stringYear. In this case, R will take care of making this a factor for you
##
#########################################


newTransformFunc <- function(dataList) {
	
	dataList[["F_creditScore"]] <- cut(x=dataList[["creditScore"]], breaks = c(700,750, 800, 850, 900))
	dataList[["stringYear"]] <- as.character(dataList[["year"]])
	return(dataList)
	
}

smallMort <- file.path(workDir, "smallerMortgages.xdf")

rxDataStep(inData=mortData, 
	       outFile = smallMort, 
	       rowSelection=creditScore>=700,  
		   transformFunc=newTransformFunc, 
		   overwrite=TRUE)



rxGetInfo(smallMort, getVarInfo=TRUE, varsToDrop=names(rxGetVarInfo(mortData)), numRows=5, startRow=1000)


#----------------------------------------------------------------------------------
## Section 3
## recoding data: factors
## Factors are special: They require knowledge of all the values in the data.

## In simple situations, you can create factors on the fly
## - for integers: use F()
rxCrossTabs(~F(year), data=mortData)

## More complicated factor manipulations can sometimes be handled using R's factor function
## inside of rxDataStep, but this won't always work!! 
## Again, because we are not seeing all of the data, all of the levels in each chunk
## example: 
manualFactorRecoding <- function(dataList) {
	dataList[["factorYear"]] <- factor(dataList[["year"]])
	return(dataList)
	
}

## This will fail:
rxDataStep(inData=mortData, 
	       outFile = file.path(workDir,"mort2"), 
	       transformVars = "year",
	       transformFunc=manualFactorRecoding,  
	       overwrite=TRUE)

## The recommended way to work with factors in RevoScaleR is to use
## the rxFactors function
## We do this in two steps:
##		1. make a new character variable using rxDatastep
##      2. use rxFactors to convert the string variable to a factor

rxDataStep(inData=mortData, 
	       outFile=mortData, 
	       transforms=list(stringYear=as.character(year)), 
	       append="cols", 
		   overwrite=TRUE)

rxGetInfo(mortData, getVarInfo=TRUE)

rxFactors(inData=mortData, 
	      outFile=mortData, 
		  factorInfo=list(factorYear2 = list(levels=c(2000:2009), varName="stringYear")), 
		  overwrite=TRUE)

rxGetInfo(mortData, getVarInfo=TRUE)

rxCrossTabs(~F(year):factorYear2, data=mortData) # resulting levels will match 
#----------------------------------------------------------------------------------------
## Section 4
## Sorting & Merging Big data

rxSortXdf(inFile=mortData,
	   outFile="SortMortData",
	   sortByVars=c("creditScore"),
	   decreasing=c(TRUE),
	   overwrite=TRUE)

rxGetInfo("SortMortData", getVarInfo=TRUE,numRows=5)

## THE MERGE FUNCTION
## This function supports a number of types of merge:
	# Inner
	# Outer: left, right, and full
	# One-to-One
	# Union

## Create first data frame
acct <- c(0538, 0538, 0538, 0763, 1534)
billee <- c("Rich C", "Rich C", "Rich C", "Tom D", "Kath P")
patient <- c(1, 2, 3, 1, 1)
acctDF<- data.frame( acct=acct, billee= billee, patient=patient)
acctDF

## Create second data frame
acct <- c(0538, 0538, 0538, 0538, 0763, 0763, 0763)
patient <- c(3, 2, 2, 3, 1, 1, 2)
type <- c("OffVisit", "AdultPro", "OffVisit", "2SurfCom", "OffVisit", "AdultPro", "OffVisit")
procedureDF <- data.frame(acct=acct, patient=patient, type=type)
procedureDF

## INNER MERGE
## All records that don't have matches are ommitted
rxMerge(inData1 = acctDF, inData2 = procedureDF, type = "inner",matchVars=c("acct", "patient"))

## OUTER MERGES
## Left outer merge
## All records from the first data frame are included
rxMerge(inData1 = acctDF, inData2 = procedureDF, type = "left",matchVars=c("acct", "patient"))

## Right outer merge
## All records from the second data frame are included
rxMerge(inData1 = acctDF, inData2 = procedureDF, type = "right",matchVars=c("acct", "patient"))

## Full outer merge
## All records from both files are included
rxMerge(inData1 = acctDF, inData2 = procedureDF, type = "full",matchVars=c("acct", "patient"))
rxMerge(inData1 = acctDF, inData2 = procedureDF, type = "full",matchVars=c("acct"))

## ONE-TO-ONE MERGE: The two data files must have the same number of rows
## First data frame
myData1 <- data.frame( x1 = 1:3, y1 = c("a", "b", "c"), z1 = c("x", "y", "z"))
myData1
## Second data frame
myData2 <- data.frame( x2 = 101:103, y2 = c("d", "e", "f"),z2 = c("u", "v", "w"))
myData2
## one-to-one merge
rxMerge(inData1 = myData1, inData2 = myData2, type = "oneToOne")

## UNION MERGE:The two data files must have the same number of columns
names(myData2) <- c("x1", "x2", "x3")
rxMerge(inData1 = myData1, inData2 = myData2, type = "union")

## MERGE TWO XDF FILES
rxMerge(inData1 = mortData, 
	    inData2 = mortData, 
		outFile = "mortTwice.xdf",
		type = "union",
		overwrite=TRUE)
	
rxGetInfo("mortTwice.xdf",getVarInfo=TRUE)

## MERGE A DATA FRAME AND AN XDF FILE
censusWorkers <- file.path(rxGetOption("sampleDataDir"), "censusWorkers.xdf")
rxGetVarInfo(censusWorkers)

## Create a data frame with per capita expenditures for each state
educExp <- data.frame(state=c("Connecticut", "Washington", "Indiana"),EducExp = c(1795.57,1170.46,1289.66 ))
educExp
## Merge in the new variable
rxMerge(inData1 = censusWorkers, 
	    inData2 = educExp,
		outFile="censusWorkersEd.xdf", 
		matchVars = "state", 
		overwrite=TRUE)

rxGetVarInfo("censusWorkersED")
#----------------------------------------------------------------------
## Section 5
## Exporting big data

mortData <- file.path(workDir, "mortgages.xdf")

## rxReadXdf: "exporting" xdf to dataframe... many options
mortDataDF <- rxReadXdf(file=mortData)
head(mortDataDF)
?rxReadXdf

## rxXdfToDataFrame: "exporting" xdf to dataframe... many more options
mortDataDF2 <- rxXdfToDataFrame(file=mortData)
head(mortDataDF2)
?rxXdfToDataFrame

#########################################
##
## Hands-on Exercise:
## undo the importing of multiple csv mortgage files into a single xdf file
## use rxSplit to split mortData into 10 different csv files, one for year year
##
#########################################

## You will need:
?rxXdfToText
?rxSplit
?rxFactors

## Let's undo the importing of multiple csv files into a single xdf file
#mortData <- file.path(workDir, "mortgages.xdf")

## First, If we haven't already, convert our split by variable to a factor variable
#rxFactors(inData=mortData, 
	      #outFile=mortData, 
		  #factorInfo=list(factorYear2 = list(levels=c(2000:2009), varName="year")), 
		  #overwrite=TRUE)
	
rxGetVarInfo(mortData)

## rxSplit: split xdf into multiple text files
mortDataXDFs <- rxSplit(mortData, splitByFactor = "factorYear2", 
	outFilesBase = file.path(getwd(),"mortData"), reportProgress = 0, verbose = 0, overwrite = TRUE)
print(mortDataXDFs)

## rxXdfToText: exporting xdf to text file

for (iYear in 2000:2009) {

	## build file name for reading
	xdfBaseName <- paste("mortData.factorYear2.", iYear, ".xdf", sep="")
	xdfFileName <- file.path(workDir, xdfBaseName)
	
	## build file name for writing
	csvBaseName <- paste("mortData.factorYear2.", iYear, ".csv", sep="")
	csvFileName <- file.path(workDir, csvBaseName)
	
	## read file into xdf file
	rxXdfToText(inFile=xdfFileName, outFile=csvFileName, overwrite=TRUE)
		
}

invisible(sapply(mortDataXDFs, function(x) if (file.exists(x@file)) unlink(x@file)))

#########################################
##
## Hands-on Exercise:
## Use your mortData xdf file. Create a function and use data step's transformFunc argument 
## to add a new variable (houseAgeDecade) to the data set: one for the house's age in decades. 
## This new variable should be categorical, spanning four categories. 
##
#########################################

#sampleDataDir <- rxOptions()[["sampleDataDir"]]
#
#mortData <- file.path(workDir, "mortgages.xdf")
##unlink(mortgageXdf)
#
#firstYear <- TRUE
#myAppend="none"
#
#for (iYear in 2000:2009) {
#
	### build file name
	#csvBaseName <- paste("mortDefaultSmall", iYear, ".csv", sep="")
	#csvFileName <- file.path(sampleDataDir, csvBaseName)
	#
#
	### read file into xdf file
	#rxImport(inData=csvFileName, outFile = mortData,append=myAppend, overwrite=firstYear)
	#
	### after the first year we don't need to overwrite anymore, just need to append rows
	#firstYear <- FALSE
	#myAppend="rows"
		#
#}

houseAgeTrans <- function(dataList)
{
  dataList$houseAgeDecade <- cut(dataList$houseAge, 
                                 breaks=seq(from = 0, to = 40, by = 10),
                                 right = FALSE)
  return(dataList)
}

rxDataStep(inData  = mortData, outFile = mortData,
           transformFunc = houseAgeTrans, transformVars=c("houseAge"),
           overwrite=TRUE)

rxGetInfo(data=mortData, numRows=5, getVarInfo=TRUE)











