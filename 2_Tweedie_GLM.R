################################################
## 
## 
## GLM Tweedie Model
##
## Stanford ASE Big Data Conference 5/31/14
##
#################################################


##### TWEEDIE EXAMPLE #######################################################
# The Tweedie family of distributions provide flexible models for estimation. 
# The power parameter var.power determines the shape of the distribution 
# var.power = 
#              0 => Tweedie is a normal distribution 
#              1 => Tweedie is Poisson
#              2 => Tweedie is Gamma
#              3 => Tweedie is inverse Gaussian. 
#              between 1 and 2 => Tweedie is a compound Poisson distribution and 
#                                 is appropriate for positive data that also contains exact zeros, 
#                                 for example, insurance claims data, rainfall data, or fish-catch data. 
#              > 2 is appropriate for positive data
#
# The data for our example is subsample from the 5% sample of the U.S. 2000 census. 
# We will consider the annual cost of property insurance for heads of household ages 21 through 89, 
# and its relationship to age, sex, and region. 
# The variable “perwt” represents the probability weight for that observation. 
#
#---------------------------------------------------------------------------------
dataPath = "C:/DATA/Census Data/Census5PCT2000"
bigCensusData <- file.path(dataPath, "Census5PCT2000.xdf")
rxGetInfoXdf(bigCensusData,getVarInfo=TRUE)

# The input file has 14,058,983 records and 265 variables
#Number of observations: 14058983 
#Number of variables: 265 
#Number of blocks: 98 
#Variable information: 
#Var 1: rectype
       #2 factor levels: H P
#Var 2: year, Census year 
       #14 factor levels: 2000 1850 1860 1870 1880 ... 1950 1960 1970 1980 1990
#Var 3: datanum, Data set number 
       #Type: integer, Low/High: (1, 1)
#Var 4: serial, Type: integer, Low/High: (1, 6175965)
#Var 5: numprec, Number of person records following 
# '
# '
# '
#-----------------------------------------------------------------------------------
# Build a working file with only the variables needed

propinFile <- file.path(dataPath, "CensusPropertyIns.xdf")
rxDataStep(inData = bigCensusData, outFile = propinFile,
					rowSelection = (related == 'Head/Householder') & (age > 20) & (age < 90),
					varsToKeep = c("propinsr", "age", "sex", "region", "perwt"),
					blocksPerRead = 10, 
					overwrite = TRUE)

rxGetInfoXdf(propinFile,getVarInfo=TRUE)



# Additional Data Cleaning
# The variable region has some very long  factor level character strings, 
# and it also has a number of levels for which there 
# are no observations. We can see this using rxSummary:

rxSummary(~region, data = propinFile)

# We can use the rxFactors function rename and reduce the number of levels:
regionLevels <- list( "New England" = "New England Division",
									"Middle Atlantic" = "Middle Atlantic Division",
									"East North Central" = "East North Central Div.",
									"West North Central" = "West North Central Div.",
									"South Atlantic" = "South Atlantic Division",
									"East South Central" = "East South Central Div.",
									"West South Central" = "West South Central Div.",
									"Mountain" ="Mountain Division",
									"Pacific" ="Pacific Division")
###
rxFactors(inData = propinFile, outFile = propinFile,
					factorInfo = list(region = list(newLevels = regionLevels,
					otherLevel = "Other")),
					overwrite = TRUE)
				
			
rxSummary(~region, data = propinFile)


#-------------------------------------------------------------------------------------------------------
# BUILD A GLM (TWEEDIE MODEL)
# As a first step to analysis, let's look at a histogram of the property insurance cost:
rxHistogram(~propinsr, data = propinFile, pweights = "perwt")

# Because the histogram shows a “clump” of exact zeros in addition to a distribution of positive values,
# this appears to be a good match for the Tweedie family with a variance power parameter between 1 and 2, 

system.time(propinGlm <- rxGlm(propinsr~sex + F(age) + region,
				    pweights = "perwt", data = propinFile,
					family = rxTweedie(var.power = 1.5), dropFirst = TRUE))
				
#
# Elapsed computation time: 42.926 secs.
#
# Look at the model
summary(propinGlm)
#
#-----------------------------------------------------------------------------------------------------------
# USE THE MODEL TO MAKE PREDICTIONS FOR 2 REGIONS

### Create a prediction data set #########################
# Get the region factor levels
varInfo <- rxGetVarInfo(propinFile)
regionLabels <- varInfo$region$levels
# Create a prediction data set for region 5, all ages, both sexes
region <- factor(rep(5, times=138), levels = 1:10, labels = regionLabels)
age <- c(21:89, 21:89)
sex <- factor(c(rep(1, times=69), rep(2, times=69)), levels = 1:2, labels = c("Male", "Female"))
predData <- data.frame(age, sex, region)
#
# Create a prediction data set for region 2, all ages, both sexes
predData2 <- predData
predData2$region <-factor(rep(2, times=138), levels = 1:10,
labels = varInfo$region$levels)
#
# Combine data sets and compute predictions
predData <- rbind(predData, predData2)
head(predData)
tail(predData)
#----------------------------------------------------------------
# Use model to predict results on prediction data set
outData <- rxPredict(propinGlm, data = predData)
#-------------------------------------------------------------------------------------------
# Plot the predictions
predData$predicted <- outData$propinsr_Pred
rxLinePlot( predicted ~age|region+sex, data = predData,
title = "Predicted Annual Property Insurance Costs",
xTitle = "Age of Head of Household",
yTitle = "Predicted Costs")


