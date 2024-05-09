import java.io.IOException;
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

public class Step4 {
    // Mapper
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // decade w1 w2 - npmi

            String[] fields = value.toString().split("\t");
            String[] keyValues = fields[0].split(" ");

            String decade = keyValues[0];
            String w1 = keyValues[1];
            String w2 = keyValues[2];
            String npmi = fields[1];

            // decade w1 w2 - npmi
            context.write(new Text(String.format("%s %s %s", decade, w1, w2)), new Text(npmi));

            // decade * - npmi
            context.write(new Text(String.format("%s *", decade)), new Text(npmi));
        }
    }

    // Reducer
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private double sumNpmi;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // decade * - npmi
            if (key.toString().charAt(key.toString().length() - 1) == '*') {
                this.sumNpmi = 0;
                for (Text value : values) {
                    this.sumNpmi += Double.parseDouble(value.toString());
                }
                // decade w1 w2 - npmi
            } else {
                double minPmi = Double.parseDouble(context.getConfiguration().get("minPmi", "0"));
                double relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi", "0"));
                for (Text value : values) {
                    double npmi = Double.parseDouble(value.toString());
                    if (npmi >= minPmi || (npmi) / this.sumNpmi >= relMinPmi) { // ITS A COLLOCATION
                        // decade w1 w2 - npmi
                        context.write(key, value);
                    }
                }
            }
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String keyValue = key.toString();
            double npmi = 0;
            // decade * - npmi
            if (keyValue.charAt(keyValue.length() - 1) == '*') {
                for (Text value : values) {
                    npmi += Double.parseDouble(value.toString());
                }
                context.write(key, new Text(Double.toString(npmi)));
            } else {
                // decade w1 w2 - npmi
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
            // decade
            String decade = key.toString().split(" ")[0];
            return Math.abs(decade.hashCode() % numPartitions);
        }
    }

    // Main
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        // conf.set("mapred.max.split.size", "33554432");
        conf.set("minPmi", args[0]);
        conf.set("relMinPmi", args[1]);
        Job job = Job.getInstance(conf, "Step4");
        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class); //TODO: decade * - npmi -> sum it!
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // job.setInputFormatClass(SequenceFileInputFormat.class); // MAYBE NEED TO
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://sk100ass2/output_step3"));
        FileOutputFormat.setOutputPath(job, new Path("s3://sk100ass2/output_step4"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}