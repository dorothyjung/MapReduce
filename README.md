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
*Output: (source, [[destinations]|empty HashMap|-1])  
    * Size: number of vertices

destinations: list of destination sources using commas as delimiters
min dist from source: how to account for multiple starting nodes?
visited?: 3 different statuses for node
--> 1 means already visited
--> 0 means not yet visited
--> -1 means unknown

After LoadReduce, we will output our nodes as (key, value) pairs:
All nodes will be marked with visited? = -1 and contain empty Distances HashMaps.
    	  (source, [[destinations]|empty HashMap|-1])

## BFS
### BFSMap  
*Input: (source, [[destinations]|[<source1, dist, visited>...<sourceN, dist, visited>]|known?])  
*Output: (source, [[destinations]|[<source1, dist, visited>...<sourceN, dist, visited>]|known?])

NEW ALGORITHM:

during the first iteration of BFSMap, we will choose the starting nodes with 1/denom probability. Since all the nodes will have DIST = Integer.MAX_VALUE, after this iteration:
   Starting nodes will be marked with HashMap of 1 entry <source, 0L> and visited? = 0.
	 (source, [[destinations]|{<source, 0L>}|0]).
All other nodes will remain as is.

The job of BFSMap is to change all the nodes from NOT_VISITED to VISITED; this means that it will visit all the nodes on the same level in 1 iteration of BFSMap. When it visits a node, it will:
if NOT_VISITED:
   write a new (key, value) pair for this node with VISITED but same distance HashMap
   (source, [[destinations]|{distances}|1])
   for every node in [destinations]:
       write a new (key, value) pair with NOT_VISITED but each distance in HashMap of source is distance += 1
       (source, [NULL|{<source1, dist+1>...<sourceN, dist+1>}|0])
else:
   identity mapper

Since we don't know how many nodes in total there are, or what the destination nodes map to, we leave the destinations part of the value as NULL. It will be BFSReduce's job to consolidate the list of destinations and the min distance.


### BFSReduce
*Input: (source, [[destinations]|[<source1, dist, visited>...<sourceN, dist, visited>]|known?])  
*Output: (source, [[destinations]|[<source1, dist, visited>...<sourceN, dist, visited>]|known?])

BFSReduce has 3 priorities in mind to output (key, value) pairs
-a non-null list of destination nodes
-the minimum distance to each source node of distances HashMap
-the greatest value of (visited)

e.g. for a key in a call to BFSReduce
input:
[1, [4,7,8|{}|UNKNOWN]
[1, [NULL|{<2, 3>, <3, 2>|NOT_VISITED]
output:
[1, [4,7,8|<2, 3>, <3, 2>|NOT_VISITED]

Distances HashMap:
-number of entries: max of all the entries of mapped values
-mapped value: min dist for respective starting node

## Histogram
### HistogramMap
*Input: (source, [[destinations]|{<source1, dist>...<sourceN, dist>}|visited])
	Looks only at the 2nd part of the value in the (key, value) pair  
*Output:

for each entry in distance HashMap:
    write (distance, 1)

This gets rid of the unnecessary data  

### HistogramReduce
*Input: (distance, 1)
size: total number of distances from each starting node to all other nodes in the graph  
*Output: (distance, frequency of distance)

