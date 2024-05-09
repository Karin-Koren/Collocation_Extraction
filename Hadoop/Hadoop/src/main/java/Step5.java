import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step5 {
    // Mapper
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // decade w1 w2 - npmi
            String[] fields = value.toString().split("\t");
            String[] keyValues = fields[0].split(" ");

            // decade npmi - w1 w2
            String outkey = keyValues[0] + " " + fields[1];
            String outVal = keyValues[1] + " " + keyValues[2];
            context.write(new Text(outkey), new Text(outVal));
        }
    }

    // Reducer

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyValues = key.toString().split(" ");
            for (Text value : values) {
                // decade npmi - w1 w2
                String outKey = keyValues[0] + " " + value.toString();
                String outVal = keyValues[1];
                // decade w1 w2 - npmi
                context.write(new Text(outKey), new Text(outVal));
            }
        }
    }

    // Partitioner
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String decade = key.toString().split(" ")[0];
            return Math.abs(decade.hashCode() % numPartitions);
        }
    }

    // Comparator
    public static class DescendingComparator extends WritableComparator {
        public DescendingComparator() {
            super(Text.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Text key1 = (Text) w1;
            Text key2 = (Text) w2;

            String[] key1Parts = key1.toString().split(" ");
            String[] key2Parts = key2.toString().split(" ");

            String decade1 = key1Parts[0];
            String decade2 = key2Parts[0];
            String npmi1 = key1Parts[1];
            String npmi2 = key2Parts[1];

            int decadeCompare = decade1.compareTo(decade2);
            if (decadeCompare != 0) {
                return decadeCompare;
            }

            double npmiValue1 = Double.parseDouble(npmi1);
            double npmiValue2 = Double.parseDouble(npmi2);
            return Double.compare(npmiValue2, npmiValue1); // Descending order for npmi
        }
    }

    // Main
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 5 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        // conf.set("mapred.max.split.size", "33554432");
        Job job = Job.getInstance(conf, "Step5");
        job.setJarByClass(Step5.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setSortComparatorClass(DescendingComparator.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://sk100ass2/output_step4"));
        FileOutputFormat.setOutputPath(job, new Path("s3://sk100ass2/output_step5"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}