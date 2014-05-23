#####################################################
#####################################################
## 
## RevoScaleR Class 
## 
## Stanford ASE Big Data Conference 5/31/14
## Compute Context
## - Mandelbrot set computation
## - sequential and parallel using different local back ends
##
#####################################################
######################################################

## Section 1
## The Mandelbrot Set
## a function that calculates the set membership for a single point

## Here we have two functions using only open source R functions 
## to compute and vectorize the calculations for the Mandelbrot set. 
## Then we have a function using rxExec to plot the Mandelbrot set.


mandelbrot <- function(x0, y0, lim) {
	x <- x0
	y <- y0
	
	iter <- 0
	
	while (x^2 + y^2 < 4 && iter < lim)  {
		xtemp <- x^2 - y^2 + x0
		y <- 2 * x * y + y0
		x <- xtemp
		iter <- iter + 1
	}
	
	return(iter)
	
}
 
## Vectorize computation; calculates membership for a row of points
vmandelbrot <- function(xvec, y0, lim) {
	
	return(
		unlist(lapply(xvec, mandelbrot, y0=y0, lim=lim))
	)

}
 
?rxExec
## rxExec allows any R function to be “parallelized”. 
## Its syntax is similar to the apply functions. 
## Here we are parallelizing the call our call vmandelbrot. 

## Each time this function is called it is run with the different y values 
## This is where the parallelism comes in. Each time the function is called 
## rxElemArg allows differnt cores to work on different parts of the vector y.in.

## Parallel Mandelbrot set computation using rxExec
PlotMandlebrot <- function(taskChunkSize=NULL) {
	size <- 240
	x.in <- seq(-2.0, 0.6, length.out=size)
	y.in <- seq(-1.3, 1.3, length.out=size)
	m <- 200
	
	z <- rxExec(FUN=vmandelbrot, 
		        xvec=x.in, 
				y0=rxElemArg(y.in), 
				lim=m, 
		        execObjects="mandelbrot",  
				taskChunkSize=taskChunkSize)
	
	z <- matrix(unlist(z), ncol=size)           #order the data for the image
	image(x.in, y.in, z, col=c(rainbow(m), '#000000'))
}
 
## Section 2
##  Run calculation using different back ends 

## local compute context, sequential
rxSetComputeContext("RxLocalSeq")
system.time(PlotMandlebrot())

## local compute context, parallel
rxSetComputeContext(RxLocalParallel())
system.time(PlotMandlebrot())

## change chunk size
system.time(PlotMandlebrot(taskChunkSize=10))
system.time(PlotMandlebrot(taskChunkSize=30))
system.time(PlotMandlebrot(taskChunkSize=60))
#system.time(PlotMandlebrot())

## Setting the compute context for different clusters: For Your Reference

## Setting the compute context on a MicroSoft HPC Server
myCluster <- RxHpcServer(
				headNode="cluster-head2",
				revoPath="C:\\Revolution\\R-Enterprise-Node-6.0\\R-2.14.2\\bin\\x64\\",
				shareDir="\\AllShare\\myName",
				dataPath="C:\\data")

rxSetComputeContext(myCluster)

## Setting the compute context for an LFS cluster from a Linux PC
myCluster <- RxLsfCluster(
				shareDir="/usr/share/lsf/shares/myName",
				dataPath="/usr/local/data")
				rxSetComputeContext(myCluster)

## Setting the compute context from a Windows Platform LSF client:
myLsfCluster <- RxLsfCluster(
				shareDir = "/mnt/allshare/myName", 
				clientShareDir="\\\\lsf-11\\allshare\\myName",
				clusterMpiPath = "/opt/ibm/platform_mpi/bin",
				dataPath= "/usr/local/data)
			
rxSetComputeContext(myLsfCluster)