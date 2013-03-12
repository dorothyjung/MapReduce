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
    // Marker for initial search vertex
    private static final long NEW_VERTEX = -1L;
    // Indicator for vertex destinations
    private static final String VERTEX_DESTINATION = "vertexGraph";

    // Example writable type
    public static class EValue implements Writable {

        public int exampleInt; //example integer field
        public long[] exampleLongArray; //example array of longs

        public EValue(int exampleInt, long[] exampleLongArray) {
            this.exampleInt = exampleInt;
            this.exampleLongArray = exampleLongArray;
        }

        public EValue() {
            // does nothing
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            out.writeInt(exampleInt);

            // Example of serializing an array:
            
            // It's a good idea to store the length explicitly
            int length = 0;

            if (exampleLongArray != null){
                length = exampleLongArray.length;
            }

            // always write the length, since we need to know
            // even when it's zero
            out.writeInt(length);

            // now write each long in the array
            for (int i = 0; i < length; i++){
                out.writeLong(exampleLongArray[i]);
            }
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            // example reading an int from the serialized object
            exampleInt = in.readInt();

            // example reading length from the serialized object
            int length = in.readInt();

            // Example of rebuilding the array from the serialized object
            exampleLongArray = new long[length];
            
            for(int i = 0; i < length; i++){
                exampleLongArray[i] = in.readLong();
            }

        }

        public String toString() {
            // We highly recommend implementing this for easy testing and
            // debugging. This version just returns an empty string.
            return new String();
        }

    }

    /* Writable LongArrayList */
    public class LongArrayListWritable extends Writable {
        public int length;
        public int ArrayList<Long> array;

        public LongArrayListWritable(int length, ArrayList<Long> array) {
            this.length = length;
            this.array = array;
        }

        public LongArrayListWritable() {

        }

        pubic void write(DataOutput out) throws IOException {
            int length = 0;
            if (array != null) {
                length = array.size();
            }
            out.writeInt(length);
            for (int i = 0; i < length; i++) {
                out.writeLong(array.get(i));
            }
        }

        public void readFields(DataInput in) throws IOException {
            int length = in.readInt();
            array = new ArrayList<Long>(length);
            for(int i = 0; i < length; i++) {
                array.add(i, in.readLong());
            }
        }

        public String toString() {
            String output = "[";
            for (Long i : array) {
                output = output + i + ", ";
            }
            output = output + "]";
            return output;
        }

    }

    /* The first mapper. Part of the graph loading process, currently just an 
     * identity function. Modify as you wish. */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, 
        LongWritable, LongWritable> {

        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {

            // example of getting value passed from main
            //int inputValue = Integer.parseInt(context.getConfiguration().get("inputValue"));
            context.write(key, value);
        }
    }


    /* The first reducer. This is also currently an identity function (although it
     * does break the input Iterable back into individual values). Modify it
     * as you wish. In this reducer, you'll also find an example of loading
     * and using the denom field.  
     */
    public static class LoaderReduce extends Reducer<LongWritable, LongWritable, 
        LongArrayListWritable, LongArrayListWritable> {

        public long denom;

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
            // We can grab the denom field from context: 
            //denom = Long.parseLong(context.getConfiguration().get("denom"));

            // You can print it out by uncommenting the following line:
            // System.out.println(denom);

            String valueString = new String();
            ArrayList<Long> valueList = new ArrayList<Long>();
            ArrayList<Long> keyList = new ArrayList<Long>();
            keylist.add(key.get());
            longArrayList.add(NEW_VERTEX);
            for (LongWritable value : values){            
                longArrayList.add(value.get());
                valueString = valueString + Long.toString(value.get()) + ",";
            }
            context.getConfiguration().set(VERTEX_DESTINATION + Long.toString(key.get()), valueString);
            context.write(new LongArrayListWritable(keyList.size(), keyList), new LongArrayListWritable(valueList.size(), valueList));
        }

    }


    // ------- Add your additional Mappers and Reducers Here ------- //


    /* The BFS mapper. Determines which nodes to inspect with probability 1/denorm.
     * Takes in (source, [destinations]) pairs and finds the distance from inspected node
     * to other vertices in the graph. */
    public static class BFSMap extends Mapper<LongArrayListWritable, LongArrayListWritable, 
        LongArrayListWritable, LongWritable> {

        @Override
        public void map(LongArrayListWritable key, LongArrayListWritable value, Context context)
                throws IOException, InterruptedException {
	    //do this for the first time ONLY
	    public long denom;
            denom = Long.parseLong(context.getConfiguration().get("denom"));
	    private double ref = Math.random();
	    private double prob = (double) 1 / denom;
	    if (ref < prob) {
		//perform op
		context.write(key, value);
	    }
        }
    }


    /* The BFS reducer. Takes in ([source,dest], distance) pairs and returns 1
     * pair ([source,dest], shortest distance). */
    public static class BFSReduce extends Reducer<LongArrayListWritable, LongWritable, 
        LongArrayListWritable, LongArrayListWritable> {

        public long denom;

        public void reduce(LongArrayListWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
	    //afixme
            for (LongWritable value : values){            
                context.write(key, value);
            }
        }

    }


    /* The last mapper. Maps each distance from input to 1. */
    public static class HistoMap extends Mapper<LongArrayListWritable, LongArrayListWritable, 
        LongWritable, LongWritable> {

        @Override
        public void map(LongArrayListWritable key, LongArrayListWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(value, 1);
        }
    }


    /* The histogram reducer. Adds up the number of occurrences of each distance.  
     */
    public static class HistoReduce extends Reducer<LongWritable, LongWritable, 
        LongWritable, LongWritable> {

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
          
	    int sum = 0;
            for (LongWritable value : values){       
		sum += value;
	    }
	    context.write(key, value);
        }

    }

    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Pass in denom command line arg:
        conf.set("denom", args[2]);

        // Sample of passing value from main into Mappers/Reducers using
        // conf. You might want to use something like this in the BFS phase:
        // See LoaderMap for an example of how to access this value
        conf.set("inputValue", (new Integer(5)).toString());

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");

        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongArrayListWritable.class);
        job.setOutputValueClass(LongArrayListWritable.class);

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
            job.setMapOutputKeyClass(LongArrayListWritable.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(LongArrayListWritable.class);
            job.setOutputValueClass(LongArrayListWritable.class);

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
