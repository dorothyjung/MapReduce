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
    private static final int VISITED = 1;
    private static final int NOT_VISITED = 0;
    private static final int UNKNOWN = -1;
    // Example writable type
    public static class VertexValueWritable implements Writable {

        public long distance; 
        public ArrayList<Long> destinations; 
        public int visited;
        private int length;

        public VertexValueWritable(ArrayList<Long> destinations, long distance, int visited) {
            this.distance = distance;
            this.destinations = destinations;
            this.visited = visited;
        }

        public VertexValueWritable() {
            // does nothing
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            out.writeLong(distance);
            out.writeInt(visited);
            length = 0;
            if (destinations != null){
                length = destinations.size();
            }
            out.writeInt(length);
            for (int i = 0; i < length; i++){
                out.writeLong(destinations.get(i));
            }
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            this.distance = in.readLong();
            this.visited = in.readInt();
            this.length = in.readInt();
            destinations = new ArrayList<Long>(length);
            for(int i = 0; i < length; i++){
                destinations.add(i, in.readLong());
            }
        }

        public String toString() {
            String stringRep = "Node\n======\nVisited: " 
                + visited + "\nDistance: " + distance + "\nDestinations: [";
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
            for (LongWritable value : values){            
                destinations.add(value.get());   
            }
            context.write(key, new VertexValueWritable(destinations, Long.MAX_VALUE, UNKNOWN));
        }

    }


    // ------- Add your additional Mappers and Reducers Here ------- //


    /* The BFS mapper. Determines which nodes to inspect with probability 1/denorm.
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
                    if (Math.random() < 1 / denom) {
                        context.write(key, new VertexValueWritable(value.destinations, 0L, NOT_VISITED));
                    }else {
                        context.write(key, value);
                    }
                }else if (value.visited == NOT_VISITED) {
                    context.write(key, new VertexValueWritable(value.destinations, value.distance, VISITED));
                    for (Long n : value.destinations) {
                        context.write(new LongWritable(n), new VertexValueWritable(null, value.distance + 1, NOT_VISITED));
                    }
                }else {
                    context.write(key, value);
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
            int minDistance = Long.MAX_VALUE;
            int maxFlag = -1;
            ArrayList<Long> destinations = new ArrayList<Long>();
            for (VertexValueWritable value : values) {
                System.out.println("Value: " + value.toString());
                if (value.distance < minDistance) {
                    minDistance = value.distance;
                }
                if (value.visited > maxFlag) {
                    maxFlag = value.visited;
                }
                if (value.destinations != null) {
                    destinations = value.destinations;
                }
            }
            context.write(key, new VertexValueWritable(destinations, minDistance, maxFlag));
        }

    }


    /* The last mapper. Maps each distance from input to 1. */
    public static class HistoMap extends Mapper<LongWritable, VertexValueWritable, 
        LongWritable, LongWritable> {

        @Override
        public void map(LongWritable key, VertexValueWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(new LongWritable(value.distance), new LongWritable(1L));
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
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

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
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
