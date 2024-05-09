import java.io.IOException;
import java.util.function.DoubleToIntFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step2 {
    // Mapper
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        // @Override
        // public void map(LongWritable key, Text value, Context context) throws
        // IOException, InterruptedException {
        // // decade w1 w2 - sumOcc P log(N)
        // String[] fields = value.toString().split("\t");
        // String[] keyValues = fields[0].split(" ");
        // String[] valsValues = fields[1].split(" ");

        // String decade = keyValues[0];
        // String w1 = keyValues[1];
        // String w2 = keyValues[2];
        // String sumOcc = valsValues[0];
        // String p = valsValues[1];
        // String logN = valsValues[2];

        // // decade w1 * - sumOcc
        // context.write(new Text(String.format("%s %s *", decade, w1)), new
        // Text(sumOcc));
        // // decade w1 w2 - sumOcc P log(N)
        // context.write(new Text(String.format("%s %s %s", decade, w1, w2)),
        // new Text(String.format("%s %s %s", sumOcc, p, logN)));

        // }
        // }
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // decade w1 w2 - sumOcc N
            String[] fields = value.toString().split("\t");
            String[] keyValues = fields[0].split(" ");
            String[] valsValues = fields[1].split(" ");

            String decade = keyValues[0];
            String w1 = keyValues[1];
            String w2 = keyValues[2];
            String sumOcc = valsValues[0];
            String n = valsValues[1];

            // decade w1 * - sumOcc
            context.write(new Text(String.format("%s %s *", decade, w1)), new Text(sumOcc));
            // decade w1 w2 - sumOcc n
            context.write(new Text(String.format("%s %s %s", decade, w1, w2)),
                    new Text(String.format("%s %s", sumOcc, n)));

        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private int cw1;

        // @Override
        // public void reduce(Text key, Iterable<Text> values, Context context) throws
        // IOException, InterruptedException {

        // String keyValue = key.toString();

        // // decade w1 * - sumOcc
        // if (keyValue.charAt(keyValue.length() - 1) == '*') {
        // this.cw1 = 0;
        // for (Text value : values) {
        // this.cw1 += Long.parseLong(value.toString());
        // }
        // }
        // // decade w1 w2 - sumOcc P log(N)
        // else {
        // String[] keyVals = keyValue.split(" ");
        // String decade = keyVals[0];
        // String w1 = keyVals[1];
        // String w2 = keyVals[2];
        // for (Text value : values) {
        // String keyOut = String.format("%s %s %s", decade, w1, w2);
        // String valOut = value.toString() + " " + Math.log(this.cw1);
        // // decade w1 w2 - sumOcc P log(N) log(c(w1))
        // context.write(new Text(keyOut), new Text(valOut));
        // }
        // }
        // }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String keyValue = key.toString();

            // decade w1 * - sumOcc
            if (keyValue.charAt(keyValue.length() - 1) == '*') {
                this.cw1 = 0;
                for (Text value : values) {
                    this.cw1 += Integer.parseInt(value.toString());
                }
            }
            // decade w1 w2 - sumOcc n
            else {
                String[] keyVals = keyValue.split(" ");
                String decade = keyVals[0];
                String w1 = keyVals[1];
                String w2 = keyVals[2];
                for (Text value : values) {
                    String keyOut = String.format("%s %s %s", decade, w1, w2);
                    String valOut = value.toString() + " " + this.cw1;
                    // decade w1 w2 - sumOcc n c(w1)
                    context.write(new Text(keyOut), new Text(valOut));
                }
            }
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String keyValue = key.toString();
            int cw1 = 0;
            // decade w1 * - sumOcc
            if (keyValue.charAt(keyValue.length() - 1) == '*') {
                for (Text value : values) {
                    cw1 += Integer.parseInt(value.toString());
                }
                context.write(key, new Text(Integer.toString(cw1)));

            } else { // TODO: change the comment if needed
                // decade w1 w2 - sumOcc P log(N)
                for (Text value : values) {
                    context.write(key, value);
                }
            }
        }
    }

    // Partitioner
    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            // decade w1
            String newKey = key.toString().split(" ")[0] + key.toString().split(" ")[1];
            return Math.abs(newKey.hashCode() % numPartitions);
        }
    }

    // Main
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        // conf.set("mapred.max.split.size", "33554432");
        Job job = Job.getInstance(conf, "Step2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class); //TODO: add the sumOcc for w1 *
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // job.setInputFormatClass(SequenceFileInputFormat.class); // MAYBE NEED TO
        job.setInputFormatClass(TextInputFormat.class);

        TextInputFormat.addInputPath(job, new Path("s3://sk100ass2/output_step1"));
        FileOutputFormat.setOutputPath(job, new Path("s3://sk100ass2/output_step2"));
        System.out.println("[DEBUG] STEP 2 finish!");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
