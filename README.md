MapReduce
=========

We can create a separate class to represent the Vertex object, which will handle the parsing of the LongWritable VALUE when we pass it into BFS reduce, since we don't want our code to get all messy. VALUE can just be a really long string, and we can use commas as delimiters for the destination vertices and some other delimiter to separate DISTANCE and VISITED_STATUS.

For the sake of terminology, vertex/vertices = node/nodes

## Graph Loading
### LoadMap
*Input: (source, destination)  
    * Size: number of edges  
*Output: (source, destination)   
    * Size: number of edges  
This is just an identity mapper.  

### LoadReduce
*Input: (source, [list of destinations])  
    * Size: number of vertices  
*Output: (source, [[destinations]|min dist from starting node|visited?])
	 (source, [[destinations]|Integer.MAX_VALUE|-1])  
    * Size: number of vertices

destinations: list of destination sources using commas as delimiters
min dist from source: how to account for multiple starting nodes?
visited?: 3 different statuses for node
--> 1 means already visited
--> 0 means not yet visited
--> -1 means unknown

After LoadReduce, we will output our nodes as (key, value) pairs:
All nodes will be marked with dist = Integer.MAX_VALUE and visited? = -1.
    	  (source, [[destinations]|Integer.MAX_VALUE|-1])

## BFS
### BFSMap  
*Input: (source, [[destinations]|min dist to source|visited?])  
*Output: (source, [[destinations]|min dist to source|visited?])
---------------------------------
(old work:)
if value = -1:
	split [source], [-1, nodes] into multiple ([source, dest], 1) pairs
	create ([source], 0) as a marker
else:
	advance node to next level
	get nodes(dest) from graph in config 
---------------------------------

NEW ALGORITHM:

In the first iteration of BFSMap, we will choose the starting nodes with 1/denom probability. Since all the nodes will have DIST = Integer.MAX_VALUE, after this iteration:
   Starting nodes will be marked with dist = 0 and visited? = 0.
	 (source, [[destinations]|0|0]).
All other nodes will remain as is.

The job of BFSMap is to change all the nodes with visited? = 0 to visited? = 1; this means that it will visit all the nodes on the same level in 1 iteration of BFSMap. When it visits a node, it will write:
    if visited?= 0:
       write a new (key, value) pair for this node with visited?= 1 but same distance = 0
       for every node in [destinations]:
       	   write a new (key, value) pair with visited?= 0 but with distance += 1
	   (source, [NULL|0|distance+=1])
    if visited?= 1 or -1:
       identity mapper

Since we don't know how many nodes in total there are, or what the destination nodes map to, we leave the destinations part of the value as NULL. It will be BFSReduce's job to consolidate the list of destinations and the min distance.


### BFSReduce
*Input: (source, [[destinations]|min dist to source|visited?])  
*Output: (source, [[destinations]|min dist to source|visited?])

BFSReduce has 3 priorities in mind to output (key, value) pairs
-a non-null list of destination nodes
-the minimum distance to source
-the greatest value of (visited?)

e.g. for a key in a call to BFSReduce
input:
[1, [4,7,8|Integer.MAX_VALUE|-1]
[1, [NULL|1|0]
output:
[1, [4,7,8|1|0]

## Histogram
### HistogramMap
*Input: (source, [[destinations]|min dist to source|visited?])
	Looks only at the 2nd part of the value in the (key, value) pair  
*Output: (distance, 1)  
This gets rid of the unnecessary data  

### HistogramReduce
*Input: (distance, 1)
size: total number of distances from each starting node to all other nodes in the graph  
*Output: (distance, frequency of distance)

