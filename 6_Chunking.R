#####################################################
#####################################################
##
## RevoScaleR Class
##
## Stanford ASE Big Data Conference 5/31/14
## 
## Advanced Topics:
## - inter-chunk communication
## - building your own chunking algorithms
##
#####################################################
#####################################################

## In this session we are going to write our own chunking function. 
## We will create a new lagged variable for the Dow Jones daily index 
## data that we used earlier.

## filenames, location, etc. 
sampleDataDir <- rxOptions()[["sampleDataDir"]]
dir(sampleDataDir)

workDir <- getwd()

DJXdf <- file.path(sampleDataDir, "DJIAdaily.xdf")
rxGetInfo(DJXdf, getVarInfo=TRUE,numRows=20)

rxSummary(~Close, data=DJXdf)   # How many missing values?

## Section 1
## Lagged variables (crossing chunk boundaries)

## first, we sort
DJXdfSorted <- file.path(workDir, "DJIAdaily.xdf")
unlink(DJXdfSorted)
DJXdfSorted <- rxSort(inData=DJXdf, 
	           outFile=DJXdfSorted, 
			   sortByVars="DaysSince1928",
			   overwrite=TRUE)

rxGetInfo(DJXdfSorted, getVarInfo=TRUE,numRows=20)	
		
## then define a transform function
buildLaggedVar1 <- function(dataList) {
	
	## without knowing what's in the previous chunk, the first observation
	## is missing
	dataList[[laggedVarName]] <- c(NA, 
		dataList[[varToBeLagged]][1:(length(dataList[[varToBeLagged]])-1)])
		
	return(dataList)
	
}

## call it using rxDataStepXdf
rxDataStep(inData=DJXdfSorted, 
	outFile=DJXdfSorted, 
	transformVars = "Close", 
	transformObjects = list(varToBeLagged = "Close", laggedVarName = "yesterdayClose"), 
	transformFunc = buildLaggedVar1, 
	append="cols", 
	overwrite=TRUE)

rxGetInfo(DJXdfSorted, getBlockSize=TRUE,getVarInfo=TRUE,numRows=5)

#Number of observations: 20636 
#Number of variables: 14 
#Number of blocks: 4 
#Rows per block: 5159 5159 5159 5159
#Variable information: 
#Var 1: Date, Type: character
#Var 2: Open, Type: numeric, Storage: float32, Low/High: (41.6300, 14165.0195)
#Var 3: High, Type: numeric, Storage: float32, Low/High: (42.6100, 14279.9600)
#Var 4: Low, Type: numeric, Storage: float32, Low/High: (40.5600, 13980.9004)
#Var 5: Close, Type: numeric, Storage: float32, Low/High: (41.2200, 14164.5303)
#Var 6: Volume, Type: numeric, Storage: float32, Low/High: (130000.0000, 11456230400.0000)
#Var 7: Adj.Close, Type: numeric, Storage: float32, Low/High: (41.2200, 14164.5303)
#Var 8: Year, Type: integer, Low/High: (1928, 2010)
#Var 9: Month, Type: integer, Low/High: (1, 12)
#Var 10: DayOfMonth, Type: integer, Low/High: (1, 31)
#Var 11: DayOfWeek
       #5 factor levels: Monday Tuesday Wednesday Thursday Friday
#Var 12: DaysSince1928, Type: integer, Low/High: (274, 30286)
#Var 13: YearFrac, Type: numeric, Storage: float32, Low/High: (1928.7501, 2010.9186)
#Var 14: yesterdayClose, Type: numeric, Low/High: (41.2200, 14164.5303)
#Data (5 rows starting with row 1):
        #Date   Open   High    Low  Close  Volume Adj.Close Year Month
#1 1928-10-01 239.43 242.46 238.24 240.01 3500000    240.01 1928    10
#2 1928-10-02 240.01 241.54 235.42 238.14 3850000    238.14 1928    10
#3 1928-10-03 238.14 239.14 233.60 237.75 4060000    237.75 1928    10
#4 1928-10-04 237.75 242.53 237.72 240.00 4330000    240.00 1928    10
#5 1928-10-05 240.00 243.08 238.22 240.44 4360000    240.44 1928    10
  #DayOfMonth DayOfWeek DaysSince1928 YearFrac yesterdayClose
#1          1    Monday           274 1928.750             NA
#2          2   Tuesday           275 1928.753         240.01
#3          3 Wednesday           276 1928.756         238.14
#4          4  Thursday           277 1928.758         237.75
#5          5    Friday           278 1928.761         240.00
#> RevoMods:::revoFix(closeDF)


rxSummary(~yesterdayClose, data=DJXdfSorted)   # How many missing values?


# Look at Close and yesterdayClose
# Everything looks fine in the beginning, but what is happening at the boundary
# at 5159 records?

closeDF <- rxXdfToDataFrame(DJXdfSorted,varsToKeep=c("Close","yesterdayClose"))
closeDF[5155:5165,]

#> closeDF[5155:5165,]
      #Close yesterdayClose
#5155 173.49         174.14
#5156 172.32         173.49
#5157 171.49         172.32
#5158 171.84         171.49
#5159 171.95         171.84
#5160 171.53             NA
#5161 168.36         171.53
#5162 167.98         168.36
#5163 168.15         167.98
#5164 167.24         168.15
#5165 165.15         167.24

# So chunk N+1 does not have the last value of yesterdayClose computed in chunk N.

## We can use some internal chunk variables to find a solution
## We can obtain some info about the chunk of data we are currently dealing with
##   .rxStartRow: The row number from the original data that was read as the first row of the current chunk.
##   .rxChunkNum: The current chunk being processed.
##   .rxReadFileName: The name of the .xdf file currently being read.
##   .rxIsTestChunk: Whether the chunk being processed is being processed as a test sample of data.
##   .rxSet:  Set the value of an object in the transform environment, e.g., .rxSet("myObject", pi). 

?rxTransform

## then define a transform function
buildLaggedVar2 <- function(dataList) {
	
	## the first observation will be missing, but only the first
	if(.rxStartRow==1) {
		
		dataList[[laggedVarName]] <- c(NA, 
			dataList[[varToBeLagged]][1:(length(dataList[[varToBeLagged]])-1)])
		
	## if this chunk's first row isn't the overall first row, we can 
	} else {
	
		dataList[[laggedVarName]] <- c(.rxGet("lastValuePrev"), 
			dataList[[varToBeLagged]][1:(length(dataList[[varToBeLagged]])-1)])
		
	}
	
	## make sure to save the last value for the next chunk
	.rxSet("lastValuePrev", dataList[[varToBeLagged]][length(dataList[[varToBeLagged]])])
	
	return(dataList)
	
}

rxDataStep(inData=DJXdfSorted, 
	outFile=DJXdfSorted, 
	transformVars = "Close", 
	transformObjects = list(varToBeLagged = "Close", laggedVarName = "yesterdayClose"), 
	transformFunc = buildLaggedVar2, 
	append="cols", 
	overwrite=TRUE)

rxSummary(~yesterdayClose, data=DJXdfSorted) # how many missing observations?

closeDF <- rxXdfToDataFrame(DJXdfSorted,varsToKeep=c("Close","yesterdayClose"))
closeDF[5155:5165,] # what happens when we cross the block boundary at record 5159?
##> closeDF[5155:5165,] 
##     Close yesterdayClose
##5155 173.49         174.14
##5156 172.32         173.49
##5157 171.49         172.32
##5158 171.84         171.49
##5159 171.95         171.84
##5160 171.53         171.95
##5161 168.36         171.53
##5162 167.98         168.36
##5163 168.15         167.98
##5164 167.24         168.15
##5165 165.15         167.24

#----------------------------------------------------------------------------------------
## Section 2
## Building your own chunk-based algorithms

## read in one chunk at a time, do something to it, move on to the next
## (calculate likelihood, sum up transformed varibale, 
##  calc. the state of a system after complicated transitions...)

## Example 1: Simple. Count rows in each chunk
## Show how to open a general XDF data source, do some work and then properly close the data source
dataSource <- RxXdfData(DJXdfSorted, varsToKeep=c("Close", "Open"))

rxOpen(src=dataSource, mode = "r")

currentData <- rxReadNext(src=dataSource)
chunkNum <- 1

while (nrow(currentData) > 0 ) {
	
	print(paste("Chunk ", chunkNum, ", Number of rows:", nrow(currentData), sep=""))
	
	currentData <- rxReadNext(src=dataSource)
	chunkNum <- chunkNum + 1

}
	
rxClose(src=dataSource)


## Example 2
## Linear Regression!

## using preexisting tools to get the right answer first. 
## Run a regression: Dependent Var: Close, Independent Var: Open
linModel <- rxLinMod(Close~Open, data=DJXdfSorted)
print(summary(linModel))

## we could build it ourselves:
## reminder: beta = Inv(X'X) * X'Y

rxOpen(src=dataSource)

chunkMatrices = function(Source=dataSource){
		chunkNum <- 1
		currentData <- rxReadNext(src=Source)
		XpX <- matrix(0,ncol=2, nrow=2)
		XpY <- matrix(0,ncol=1, nrow=2)

		while (nrow(currentData) > 0 ) {
			
			print(paste("Chunk ", chunkNum, ", Number of rows:", nrow(currentData), sep=""))
			
			XmatCurrent <- as.matrix(currentData$Open, ncol=1)
			XmatCurrent <- cbind(1, XmatCurrent)
			YmatCurrent <- as.matrix(currentData$Close, ncol=1)
			
			XpX <- XpX + crossprod(XmatCurrent, XmatCurrent)
			XpY <- XpY + crossprod(XmatCurrent, YmatCurrent)
			
			currentData <- rxReadNext(src=dataSource)
			chunkNum <- chunkNum + 1
			

	       }
	results <- list(XpX,XpY)
	return(results)
	
}	


matrixProducts <- chunkMatrices()
rxClose(src=dataSource)

XX <- as.matrix(matrixProducts[[1]])
XY <- as.matrix(matrixProducts[[2]])

## and now, to our home brewed solution:
solve(XX, XY)

## check your work!
print(summary(linModel))

#########################################
##
## Hands on Exercise
## Use your  mortgage default xdf file created in the data manipulation lab 
## to compute the average credit card debt (ccDebt) for observations corresponding 
## to each of the four house age categories 
## (houseAgeDecade): [0,10), [10,20), [20,30), [30,40).
##
## This problem is modeled after the example for Writing Your Own Analyses for Large Data Sets. 
## See the RevoScaleR R User Guide for a big hint on how to proceed.
##   
#########################################


# Do this by writing your own chunking function and verifying the result using rxCrossTabs.
workDir <- getwd()
sampleDataDir <- rxOptions()[["sampleDataDir"]]

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


houseAgeTrans <- function(dataList)
{
  dataList$houseAgeDecadeF1 <- cut(dataList$houseAge, 
                                 breaks=seq(from = 0, to = 39, by = 10),
                                 right = FALSE)
  return(dataList)
}

rxDataStep(inData  = mortData, outFile = mortData,
           transformFunc = houseAgeTrans, transformVars=c("houseAge"),
           overwrite=TRUE)

rxGetInfo(mortData, getVarInfo=TRUE, numRows=5)

## This problem is modeled after the example for Writing Your Own Analyses for Large Data Sets. 
## See the RevoScaleR R User Guide for a big hint on how to proceed.

rxCrossTabs(ccDebt ~ houseAgeDecadeF1, data = mortData, means=T)

ProcessAndUpdateData <- function( data )
{
  # Process Data
  notMissing <- !is.na(data$ccDebt)
  
  houseAgeDecade1 <- data$houseAgeDecade == "[0,10)"  & notMissing
  houseAgeDecade2 <- data$houseAgeDecade == "[10,20)" & notMissing
  houseAgeDecade3 <- data$houseAgeDecade == "[20,30)" & notMissing
  houseAgeDecade4 <- data$houseAgeDecade == "[30,40)" & notMissing
  
  h1ccDebt <- sum(data$ccDebt[houseAgeDecade1], na.rm = TRUE)
  h1count  <- sum(houseAgeDecade1, na.rm = TRUE)
  
  h2ccDebt <- sum(data$ccDebt[houseAgeDecade2], na.rm = TRUE)
  h2count  <- sum(houseAgeDecade2, na.rm = TRUE)
  
  h3ccDebt <- sum(data$ccDebt[houseAgeDecade3], na.rm = TRUE)
  h3count  <- sum(houseAgeDecade3, na.rm = TRUE)
  
  h4ccDebt <- sum(data$ccDebt[houseAgeDecade4], na.rm = TRUE)
  h4count  <- sum(houseAgeDecade4, na.rm = TRUE)
  
  # Update Results
  .rxSet("toH1ccDebt", h1ccDebt + .rxGet("toH1ccDebt"))
  .rxSet("toH1count",  h1count  + .rxGet("toH1count"))
  
  .rxSet("toH2ccDebt", h2ccDebt + .rxGet("toH2ccDebt"))
  .rxSet("toH2count",  h2count  + .rxGet("toH2count"))
  
  .rxSet("toH3ccDebt", h3ccDebt + .rxGet("toH3ccDebt"))
  .rxSet("toH3count",  h3count  + .rxGet("toH3count"))
  
  .rxSet("toH4ccDebt", h4ccDebt + .rxGet("toH4ccDebt"))
  .rxSet("toH4count",  h4count  + .rxGet("toH4count"))
  
  return( NULL )
  
}

totalRes <- rxDataStep( inData = mortData , returnTransformObjects = TRUE,
                     transformObjects =
                       list(toH1ccDebt=0, toH1count=0, 
                            toH2ccDebt=0, toH2count=0, 
                            toH3ccDebt=0, toH3count=0, 
                            toH4ccDebt=0, toH4count=0),
                        transformFunc = ProcessAndUpdateData,
                        transformVars = c("ccDebt", "houseAgeDecade"))

  
FinalizeResults <- function(totalRes)
{
  return(data.frame(
    AveH1ccDebt = totalRes$toH1ccDebt / totalRes$toH1count,
    AveH2ccDebt = totalRes$toH2ccDebt / totalRes$toH2count,
    AveH3ccDebt = totalRes$toH3ccDebt / totalRes$toH3count,
    AveH4ccDebt = totalRes$toH4ccDebt / totalRes$toH4count))
}

FinalizeResults(totalRes)

########################

workDir <- getwd()
sampleDataDir <- rxOptions()[["sampleDataDir"]]

mortData <- file.path(workDir, "mortgages.xdf")


rxDataStep(inData=mortData,
           outFile=mortData,
           transforms = list(houseAgeDecade2=ifelse(houseAge<10,"[0,10)",
                                            ifelse(houseAge>=10 & houseAge<20,"[10,20)",
                                            ifelse(houseAge>=20 & houseAge<30,"[20,30)","[30,40]")))),
           append="cols",
           overwrite=TRUE)
	
rxGetInfo(mortData, getVarInfo=TRUE, numRows=5)

facts <- list(houseAgeDecadeF=list(varName="houseAgeDecade2"))
rxFactors(inData=mortData,
	      factorInfo=facts,
		  outFile=mortData,
		  overwrite=TRUE)

		
rxGetInfo(mortData, getVarInfo=TRUE, numRows=5)