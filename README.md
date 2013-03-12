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
*Output: (source, [destinations])  
    * Size: number of vertices  

## BFS
### BFSMap  
*Input: ??  
*Output: ([source, destination], distance)  
This outputs every path we find for a (source, destination) pair.  
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
