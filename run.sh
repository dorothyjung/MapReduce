make clean
make
rm -rf $1
rm -rf bfs*
hadoop jar sw.jar SmallWorld ~cs61c/proj2data/$1$2 $1 $3
