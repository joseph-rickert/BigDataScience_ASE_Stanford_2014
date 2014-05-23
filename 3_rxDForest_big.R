##########################################################
## Example of rxDForest
## JB Rickert
## Mortgage Default Data 10,000,000 rows
##
## Stanford ASE Big Data Conference 5/31/14
##
###########################################################
library(RevoTreeView)       # library for plotting trees
load("rxDForest.RData")

# Fetch the Mortgage Default Data
dataDir <- "C:/DATA/Mortgage Data/mortDefault"
fileName <- "mortDefault"
mData <- file.path(dataDir,fileName)
rxGetInfo(mData,getVarInfo=TRUE,numRows=5)
#--------------------------------------------------------------------------------
# Create a new data file having a variable with uniform random numbers
# going from 1 to 10. This variable will be used to create the training and test
# data sets.
rxDataStep(inData = mData, outFile = "mortDefault2",
      transforms=list(urns = as.integer(runif(.rxNumRows,1,11))),
      overwrite=TRUE)
rxGetInfo("mortDefault2",getVarInfo=TRUE,numRows=3)
#-------------------------------------------------------------------------------
# Build a training file
#
rxDataStep(inData = "mortDefault2", 
	       outFile = "mdTrain",
		   transforms=list(CS = creditScore,
						   yrE = yearsEmploy,
						   HA = houseAge),
           rowSelection = urns < 9, 
		   overwrite=TRUE )

rxGetInfo("mdTrain",getVarInfo=TRUE,numRows=5)
#---------------------------------------------------------
#  Build a test file

rxDataStep(inData = "mortDefault2", 
	       outFile = "mdTest",
		   transforms=list(CS = creditScore,
						   yrE = yearsEmploy,
						   HA = houseAge),
          rowSelection = urns > 8, 
		  overwrite=TRUE)

rxGetInfo("mdTest",getVarInfo=TRUE,numRows=5)

#--------------------------------------------------------
# Build a tree model with rxDtree to look at a sample tree
#form1 <- formula(default ~ HA + year + CS + yrE + ccDebt)
#model.tree <- rxDTree(formula = form1, data="mdTrain",maxDepth=5,cp=.01)
###
#Elapsed time for RxDTreeBase: 241.558 secs.
##
model.tree
plot(createTreeView(model.tree))
	
#------------------------------------------------------------------
# Build an rxDForest Model on the training data set
#mortForest <- rxDForest(formula = form1, data="mdTrain",maxDepth=5,cp=.01)
#mortForest
#Elapsed time for RxDTreeBase: 526.742 secs.

# Use the model to predict defaults on the test data set
rxPredict(modelObject=mortForest,
	     data="mdTest",
		 outData="mdTest2",
		 predVarNames="T_Pred")
rxGetInfo("mdTest2",getVarInfo=TRUE,numRows=5)

# Munge the data a little to get it into a form to draw the ROC curve
# Merge the predicted values into the test data set
rxMerge(inData1="mdTest2",inData2="mdTest",outFile="rocData",type="oneToOne")
#
# Add a new prediction variable for the rxRocCurve
rxDataStep(inData="rocData",
	       outFile="rocData",
	       transforms=list(T_Pred_L = as.logical(round(T_Pred))),
		   overwrite=TRUE)
#
rxGetInfo("rocData",getVarInfo=TRUE,numRows=5)
#
rxRocCurve(actualVarName = "default", 
		predVarNames = c("T_Pred"),
		data = "rocData",
		title ="ROC Curve for Forest Model of Mortgage Default Data")
	
#-----------------------------------------------------------
#save.image(file = "rxDForest.RData")
