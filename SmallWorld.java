/*
 *
 * CS61C Spring 2013 Project 2: Small World
 *
 * Partner 1 Name: Jene Li
 * Partner 1 Login: cs61c-ip
 *
 * Partner 2 Name: Yoonjung Dorothy Jung
 * Partner 2 Login: cs61c-kb
 *
 * REMINDERS: 
 *
 * 1) YOU MUST COMPLETE THIS PROJECT WITH A PARTNER.
 * 
 * 2) DO NOT SHARE CODE WITH ANYONE EXCEPT YOUR PARTNER.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 *
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SmallWorld {
    // Maximum depth for any breadth-first search
    public static final int MAX_ITERATIONS = 20;
    // flags for vertices 
    private static final int DISTANCE = 0;
    private static final int FLAG = 1;

    private static final long NOT_VISITED = 0L;
    private static final long VISITED = 1L;

    private static final int UNKNOWN = -1;
    private static final int KNOWN = 1;
    // Example writable type
    public static class VertexValueWritable implements Writable {

        public ArrayList<Long> destinations; 
	    public HashMap<Long, Long[]> distances;
        public int visited;
        private int length;
	    private int startNodes;

        public VertexValueWritable(ArrayList<Long> destinations, HashMap<Long, Long[]> distances, int visited) {
            this.distances = distances;
            this.destinations = destinations;
            this.visited = visited;
        }

        public VertexValueWritable() {
            // does nothing
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            out.writeInt(visited);
            length = 0; 
            startNodes = 0;

            if (destinations != null){
                length = destinations.size();
            }
            out.writeInt(length);
            for (int i = 0; i < length; i++){
                out.writeLong(destinations.get(i));
            }

	       if (distances != null) {
		      startNodes = distances.size();
	        }
	        out.writeInt(startNodes);
	        for (Long node : distances.keySet()) {
		      out.writeLong(node);
		      out.writeLong(distances.get(node)[0]);
              out.writeLong(distances.get(node)[1]);
	        }  
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            this.visited = in.readInt();
            this.length = in.readInt();
            this.destinations = new ArrayList<Long>(length);
	        this.distances = new HashMap<Long, Long[]>();

            for(int i = 0; i < length; i++){
                destinations.add(in.readLong());
            }

	       this.startNodes = in.readInt();
	       for (int i = 0; i < startNodes; i++) {
		      Long source = in.readLong();
              Long[] array = {in.readLong(), in.readLong()};
		      distances.put(source, array);
	        }
        }

        public String toString() {

            String stringRep = "Node\n======\nVisited: " + visited
		+ "\nDistances: " + distances.toString() + "\nDestinations: [";
            for (int i = 0; i < length; i++) {
                stringRep = stringRep + destinations.get(i) + ", ";
            }
            return stringRep + "]";
        }

    }

    /* The first mapper. Part of the graph loading process, currently just an 
     * identity function. Modify as you wish. */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, 
        LongWritable, LongWritable> {

        @Override
        public void map(LongWritable key, LongWritable value, Context context)
	    throws IOException, InterruptedException {
            context.write(key, value);
        }
    }


    /* The first reducer. This is also currently an identity function (although it
     * does break the input Iterable back into individual values). Modify it
     * as you wish. In this reducer, you'll also find an example of loading
     * and using the denom field.  
     */
    public static class LoaderReduce extends Reducer<LongWritable, LongWritable, 
        LongWritable, VertexValueWritable> {

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
            ArrayList<Long> destinations = new ArrayList<Long>();
	        HashMap<Long, Long[]> distances = new HashMap<Long, Long[]>();
            for (LongWritable value : values){            
                destinations.add(value.get());   
            }
            context.write(key, new VertexValueWritable(destinations, distances, UNKNOWN));
        }

    }


    // ------- Add your additional Mappers and Reducers Here ------- //


    /* The BFS mapper. Determines which nodes to inspect with probability 1/denom.
     * Takes in (source, [destinations]) pairs and finds the distance from inspected node
     * to other vertices in the graph. */
    public static class BFSMap extends Mapper<LongWritable, VertexValueWritable, 
        LongWritable, VertexValueWritable> {
        public long denom;

        @Override
        public void map(LongWritable key, VertexValueWritable value, Context context)
	    throws IOException, InterruptedException {
	       System.out.println("BFSMap\n=======\nKey: " + key.get() +  "\nValue: " + value.toString());
	       if (value.visited == UNKNOWN) {
                denom = Long.parseLong(context.getConfiguration().get("denom"));
                double prob = Math.random();
		        if (prob <= (1 / (double)denom)) {
                  Long[] arr = {0L, NOT_VISITED};
		          value.distances.put(key.get(), arr);
		          context.write(key, new VertexValueWritable(value.destinations, value.distances, KNOWN));//startnode
		        } else {
		          context.write(key, new VertexValueWritable(value.destinations, value.distances, KNOWN));
		        }
	        } else {
                HashMap<Long, Long[]> visitedMap = new HashMap<Long, Long[]>();
                HashMap<Long, Long[]> notVisitedMap = new HashMap<Long, Long[]>();
                for (Long source : value.distances.keySet()) {
                    Long[] dist = value.distances.get(source);
                    if (dist[FLAG] == NOT_VISITED) {
                        Long[] notArr = {dist[DISTANCE] + 1, NOT_VISITED};
                        notVisitedMap.put(source, notArr);
                    }
                    Long[] visitedArr = {dist[DISTANCE], VISITED};
                    visitedMap.put(source, visitedArr);
                }
                context.write(key, new VertexValueWritable(value.destinations, visitedMap, KNOWN));
                if (!notVisitedMap.isEmpty()) {
                    for (Long n : value.destinations) {
                        context.write(new LongWritable(n), new VertexValueWritable(null, notVisitedMap, KNOWN));
                    }
                }
            }
	
	    }
    }



    /* The BFS reducer. Takes in ([source,dest], distance) pairs and returns 1
     * pair ([source,dest], shortest distance). */
    public static class BFSReduce extends Reducer<LongWritable, VertexValueWritable, 
        LongWritable, VertexValueWritable> {
        
        public void reduce(LongWritable key, Iterable<VertexValueWritable> values, 
            Context context) throws IOException, InterruptedException {
            System.out.println("BFSReduce\n=====\nKey: " + key.get());
            HashMap<Long, Long[]> reduceMap = new HashMap<Long, Long[]>();
            ArrayList<Long> reduceDestinations = new ArrayList<Long>();
            for (VertexValueWritable v : values) {
                if (v.destinations != null) {
                    reduceDestinations = v.destinations;
                }
                for (Long source : v.distances.keySet()) {
                    if (reduceMap.containsKey(source)) {
                        Long[] reduceDist = reduceMap.get(source);
                        Long[] curDist = v.distances.get(source);
                        if (reduceDist[DISTANCE] > curDist[DISTANCE]) {
                            reduceDist[DISTANCE] = curDist[DISTANCE];
                        }
                        if (curDist[FLAG] == VISITED) {
                            reduceDist[FLAG] = VISITED;
                        }
                        reduceMap.put(source, reduceDist);
                    } else {
                        reduceMap.put(source, v.distances.get(source));
                    }
                }
            }
            context.write(key, new VertexValueWritable(reduceDestinations, reduceMap, KNOWN));
        }
    }


    /* The last mapper. Maps each distance from input to 1. */
    public static class HistoMap extends Mapper<LongWritable, VertexValueWritable, 
        LongWritable, LongWritable> {

        @Override
        public void map(LongWritable key, VertexValueWritable value, Context context)
                throws IOException, InterruptedException {
	        for (Long node : value.distances.keySet()) {
		      context.write(new LongWritable(value.distances.get(node)[DISTANCE]), new LongWritable(1L));
	        }
	    }
    }


    /* The histogram reducer. Adds up the number of occurrences of each distance.  
     */
    public static class HistoReduce extends Reducer<LongWritable, LongWritable, 
        LongWritable, LongWritable> {

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
            Long sum = 0L;
            for (LongWritable value : values) {       
		      sum += value.get();
	        }
            if (!(sum > MAX_ITERATIONS)) {
	           context.write(key, new LongWritable(sum));
            }
        }

    }

    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Pass in denom command line arg:
        conf.set("denom", args[2]);

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");

        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(LoaderReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Repeats your BFS mapreduce
        int i = 0;
        while (i < MAX_ITERATIONS) {
            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            // Feel free to modify these four lines as necessary:
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(VertexValueWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(VertexValueWritable.class);

            // You'll want to modify the following based on what you call
            // your mapper and reducer classes for the BFS phase.
            job.setMapperClass(BFSMap.class); // currently the default Mapper
            job.setReducerClass(BFSReduce.class); // currently the default Reducer

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
	    //            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));
            TextOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

            job.waitForCompletion(true);
            i++;
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        // Feel free to modify these two lines as necessary:
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        // DO NOT MODIFY THE FOLLOWING TWO LINES OF CODE:
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // You'll want to modify the following based on what you call your
        // mapper and reducer classes for the Histogram Phase
        job.setMapperClass(HistoMap.class); // currently the default Mapper
        job.setReducerClass(HistoReduce.class); // currently the default Reducer

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
	//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
