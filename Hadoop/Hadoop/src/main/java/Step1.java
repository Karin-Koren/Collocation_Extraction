import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class Step1 {

    // Mapper
    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

        // HashMap<String, Integer> stopWords = new HashMap<String, Integer>();
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputStream is = getClass().getClassLoader().getResourceAsStream("eng-stopwords.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(is));

            String line;
            while ((line = br.readLine()) != null) {
                stopWords.add(line.trim());
            }
            br.close();
        }
        // protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context
        // context)
        // throws IOException, InterruptedException {
        // super.setup(context);

        // String[] stopWordsArr = new String[] { "a", "about", "above", "across",
        // "after", "afterwards", "again",
        // "against", "all", "almost", "alone", "along", "already", "also", "although",
        // "always", "am",
        // "among", "amongst", "amoungst", "amount", "an", "and", "another", "any",
        // "anyhow", "anyone",
        // "anything", "anyway", "anywhere", "are", "around", "as", "at", "back", "be",
        // "became", "because",
        // "become", "becomes", "becoming", "been", "before", "beforehand", "behind",
        // "being", "below",
        // "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but",
        // "by", "call", "can",
        // "cannot", "cant", "co", "computer", "con", "could", "couldnt", "cry", "de",
        // "describe", "detail",
        // "do", "done", "down", "due", "during", "each", "eg", "eight", "either",
        // "eleven", "else",
        // "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone",
        // "everything",
        // "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire",
        // "first", "five", "for",
        // "former", "formerly", "forty", "found", "four", "from", "front", "full",
        // "further", "get", "give",
        // "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here",
        // "hereafter", "hereby", "herein",
        // "hereupon", "hers", "herself", "him", "himself", "his", "how", "however",
        // "hundred", "i", "ie",
        // "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself",
        // "keep", "last",
        // "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me",
        // "meanwhile", "might",
        // "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must",
        // "my", "myself",
        // "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no",
        // "nobody", "none",
        // "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often",
        // "on", "once", "one",
        // "only", "onto", "or", "other", "others", "otherwise", "our", "ours",
        // "ourselves", "out", "over",
        // "own", "part", "per", "perhaps", "please", "put", "rather", "re", "same",
        // "see", "seem", "seemed",
        // "seeming", "seems", "serious", "several", "she", "should", "show", "side",
        // "since", "sincere",
        // "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime",
        // "sometimes",
        // "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the",
        // "their", "them",
        // "themselves", "then", "thence", "there", "thereafter", "thereby",
        // "therefore", "therein",
        // "thereupon", "these", "they", "thick", "thin", "third", "this", "those",
        // "though", "three",
        // "through", "throughout", "thru", "thus", "to", "together", "too", "top",
        // "toward", "towards",
        // "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us",
        // "very", "via", "was", "we",
        // "well", "were", "what", "whatever", "when", "whence", "whenever", "where",
        // "whereafter", "whereas",
        // "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while",
        // "whither", "who",
        // "whoever", "whole", "whom", "whose", "why", "will", "with", "within",
        // "without", "would", "yet",
        // "you", "your", "yours", "yourself", "yourselves" };

        // for (int i = 0; i < stopWordsArr.length; i++) {
        // this.stopWords.put(stopWordsArr[i], 0);
        // }
        // }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split("\t");
            String[] twoGram = fields[0].split(" ");
            if (twoGram.length == 2 && fields.length > 2) {
                String w1 = twoGram[0];
                String w2 = twoGram[1];
                // dump the stop words and the jibrish words
                if (!(this.stopWords.contains(w1) || this.stopWords.contains(w2) || !w1.matches("[a-zA-Z]+")
                        || !w2.matches("[a-zA-Z]+"))) {

                    IntWritable occurrences = new IntWritable(Integer.parseInt(fields[2]));
                    String decade = fields[1].substring(0, fields[1].length() - 1) + "0";

                    // decade w1 w2 - occ
                    context.write(new Text(String.format("%s %s %s", decade, w1, w2)), occurrences);
                    // decade * - occ
                    context.write(new Text(decade + " *"), occurrences);
                }
            }
        }
    }

    // public static class ReducerClass extends Reducer<Text, IntWritable, Text,
    // Text> {
    // private int n = 0;

    // @Override
    // public void reduce(Text key, Iterable<IntWritable> values, Context context)
    // throws IOException, InterruptedException {

    // String keyValue = key.toString();

    // // decade * - occ
    // if (keyValue.contains("*")) {
    // // calc n
    // this.n = 0;
    // for (IntWritable value : values) {
    // this.n += value.get();
    // }
    // // decade w1 w2 - occ
    // } else {
    // int sumOcc = 0;
    // for (IntWritable value : values) {
    // sumOcc += value.get();
    // }
    // // decade w1 w2 - sumOcc N
    // context.write(key, new Text(String.format("%d %d", sumOcc, this.n))); //
    // sumOcc + " " + this.n));
    // }
    // }
    // }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, Text> {
        private int n = 0;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // Calculate sumOcc for each key
            int sumOcc = 0;
            for (IntWritable value : values) {
                sumOcc += value.get();
            }

            // If the key is in the format "decade *", update the global count 'n'
            if (key.toString().endsWith(" *")) {
                this.n = sumOcc; // Assuming sumOcc represents the global count
            } else {
                // Emit the key and the combined information (sumOcc and n)
                String outputValue = String.format("%d %d", sumOcc, this.n);
                context.write(key, new Text(outputValue));
            }
        }
    }

    public static class CombinerClass extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

            int sum = 0;

            // calc n
            for (IntWritable value : values) {
            sum += value.get();
            }

            // decade w1 w2 - occ
            // decade * - occ
            context.write(key, new IntWritable(sum));
            }
    }

    // Partitioner
    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String decade = key.toString().split(" ")[0];
            return Math.abs(decade.hashCode() % numPartitions);
        }

    }

    // Main
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //conf.set("mapred.max.split.size", "33554432");
        Job job = Job.getInstance(conf, "Step1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // job.setInputFormatClass(TextInputFormat.class);

        // TextInputFormat.addInputPath(job, new Path("s3://ks100ass2/textfile1"));
        job.setInputFormatClass(SequenceFileInputFormat.class);

        TextInputFormat.addInputPath(job,
                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-1M/2gram/data"));

        FileOutputFormat.setOutputPath(job, new Path("s3://sk100ass2/output_step1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
