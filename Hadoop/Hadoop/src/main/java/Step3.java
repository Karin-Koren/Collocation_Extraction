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

public class Step3 {
    // Mapper
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        // @Override
        // public void map(LongWritable key, Text value, Context context) throws
        // IOException, InterruptedException {
        // // decade w1 w2 - sumOcc P log(N) log(c(w1))
        // String[] fields = value.toString().split("\t");
        // String[] keyValues = fields[0].split(" ");
        // String[] valsValues = fields[1].split(" ");

        // String decade = keyValues[0];
        // String w1 = keyValues[1];
        // String w2 = keyValues[2];
        // String sumOcc = valsValues[0];
        // String p = valsValues[1];
        // String logN = valsValues[2];
        // String logCW1 = valsValues[3];

        // // decade w2 * - sumOcc
        // context.write(new Text(String.format("%s %s *", decade, w1)), new
        // Text(sumOcc));
        // // decade w2 w1 - sumOcc P log(N) log(c(w1))
        // context.write(new Text(String.format("%s %s %s", decade, w2, w1)),
        // new Text(String.format("%s %s %s %s", sumOcc, p, logN, logCW1)));
        // }
        // }
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // decade w1 w2 - sumOcc n c(w1)
            String[] fields = value.toString().split("\t");
            String[] keyValues = fields[0].split(" ");
            String[] valsValues = fields[1].split(" ");

            String decade = keyValues[0];
            String w1 = keyValues[1];
            String w2 = keyValues[2];
            String sumOcc = valsValues[0];
            String n = valsValues[1];
            String cw1 = valsValues[2];

            // decade w2 * - sumOcc
            context.write(new Text(String.format("%s %s *", decade, w2)), new Text(sumOcc));
            // decade w2 w1 - sumOcc n c(w1)
            context.write(new Text(String.format("%s %s %s", decade, w2, w1)),
                    new Text(String.format("%s %s %s", sumOcc, n, cw1)));
        }
    }

    // Reducer
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private int cw2;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String keyValue = key.toString();

            // decade w2 * - sumOcc
            if (keyValue.charAt(keyValue.length() - 1) == '*') {
                this.cw2 = 0;
                for (Text value : values) {
                    this.cw2 += Integer.parseInt(value.toString());
                }
            }
            // decade w2 w1 - sumOcc n c(w1)
            else {
                String[] keyVals = keyValue.split(" ");
                String decade = keyVals[0];
                String w2 = keyVals[1];
                String w1 = keyVals[2];
                for (Text value : values) {

                    String[] vals = value.toString().split(" ");
                    double sumOcc = Double.parseDouble(vals[0]);
                    double n = Double.parseDouble(vals[1]);
                    double cw1 = Double.parseDouble(vals[2]);
                    if (sumOcc == 1 || (sumOcc == cw1 && sumOcc == cw2)) {
                        continue;
                    }
                    // calculate npmi:
                    // pmi = log(occ) + log(n) - log(c1) - log(c2)

                    double pmi = Math.log(sumOcc) + Math.log(n) - Math.log(cw1) - Math.log(this.cw2);
                    double p = sumOcc / n;
                    if (pmi == 0 || p == 1) {
                        continue;
                    }
                    double npmi = pmi / (-1 * Math.log(p));
                    if (Double.isNaN(npmi)) {
                        continue;
                    }
                    String keyOut = String.format("%s %s %s", decade, w1, w2);
                    // decade w1 w2 - npmi
                    context.write(new Text(keyOut), new Text(Double.toString(npmi)));
                }
            }
        }
    }

    public static class CombinerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String keyValue = key.toString();
            int cw2 = 0;
            // decade w2 * - sumOcc
            if (keyValue.charAt(keyValue.length() - 1) == '*') {
                for (Text value : values) {
                    cw2 += Integer.parseInt(value.toString());
                }
                context.write(key, new Text(Integer.toString(cw2)));

            } else { // TODO: change comment
                // decade w2 w1 - sumOcc P log(N) log(c(w1))
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
            // decade w2
            String newKey = key.toString().split(" ")[0] + key.toString().split(" ")[1];
            return Math.abs(newKey.hashCode() % numPartitions);
        }
    }

    // Main
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        // conf.set("mapred.max.split.size", "33554432");
        Job job = Job.getInstance(conf, "Step3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class); //TODO: add the sumOcc for w2 *
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        // job.setInputFormatClass(SequenceFileInputFormat.class); // MAYBE NEED TO
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://sk100ass2/output_step2"));
        FileOutputFormat.setOutputPath(job, new Path("s3://sk100ass2/output_step3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}