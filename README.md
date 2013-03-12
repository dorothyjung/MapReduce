MapReduce
=========

## Graph Loading
### LoadMap
*Input: (source, destination)  
    * Size: number of edges  
*Output: (source, destination)   
    * Size: number of edges  
This is just an identity mapper.  

### LoadReduce
*Input: (source, [destination])  
    * Size: number of vertices  
*Output: ([source], [-1,nodes])  
    * Size: number of vertices  

## BFS
### BFSMap  
*Input: ([source, dest], [distance]) or ([source], [-1, nodes])  
*Output: ([source, destination], distance)  
if value = -1:
	split [source], [-1, nodes] into multiple ([source, dest], 1) pairs
	create ([source], 0) as a marker
else:
	advance node to next level
	get nodes(dest) from graph in config 

### BFSReduce
Input: ([source, destination], [distances])  
Output: ([source, destination], shortest distance)  
This takes all the paths and outputs the shortest path for each (source, destination) pair.  

## Histogram
### HistogramMap
*Input: ([source, destination], shortest distance)  
*Output: (distance, 1)  
This gets rid of the unnecessary data  
### HistogramReduce
*Input: (distance, [1's])  
*Output: (distance, frequency of distance)  
