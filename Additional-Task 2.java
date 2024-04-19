import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;

public class Task2RemoveSpecialsMapper {
  
  public static class RemoveSpecialsMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private static final Pattern pattern = Pattern.compile("[^a-zA-Z\\s]");
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString().toLowerCase().replaceAll(pattern.pattern(), "");
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word, one);
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Task2");
    job.setJarByClass(Task2RemoveSpecialsMapper.class);

    Configuration removeSpecialsConf = new Configuration(false);
    
    ChainMapper.addMapper(job,
      RemoveSpecialsMapper.class,
      Object.class, Text.class,
      Text.class, IntWritable.class,
      removeSpecialsConf);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
