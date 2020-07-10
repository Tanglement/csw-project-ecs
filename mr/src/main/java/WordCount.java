import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class WordCount {
    public WordCount() {
    }
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://bigdata:9000");
        conf.set("dfs.client.use.datanode.hostname", "true");
        conf.set("mapreduce.framework.name", "local");
        //conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        /*
        GenericOptionsParser()用于解析命令行参数的基本类，程序将args中的参数配置到conf
        getRemainingArgs()将剩下不是参数配置的存储otherArgs，这里是input地址和output地址
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if(otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        */
        //初始化Job，设定相关参数
        Job job = Job.getInstance(conf, "word count");
        //默认为Text
        job.setInputFormatClass ( TextInputFormat.class );
        job.setOutputFormatClass ( TextOutputFormat.class );
        //设置Mapper、Combiner和Reducer
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setCombinerClass(WordCount.IntSumReducer.class);
        job.setReducerClass(WordCount.IntSumReducer.class);
        //设置Mapper输出类型
        job.setMapOutputKeyClass (Text.class);
        job.setMapOutputValueClass (IntWritable.class);
        //设置Reducer输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置File输入输出路径
        FileInputFormat.addInputPath(job, new Path("/user/hadoop/input"));
        FileOutputFormat.setOutputPath(job, new Path("/user/hadoop/output"));

        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public TokenizerMapper() {
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //StringTokenizer用于分割字符串，不加第二个参数默认空格、换行符、回车符
            StringTokenizer itr = new StringTokenizer(value.toString());
            //hasMoreTokens()是否还有分隔符，nextToken返回当前位置到下一个分隔符的字符串
            while(itr.hasMoreTokens()) {
                this.word.set(itr.nextToken());
                context.write(this.word, one);
            }
        }
    }
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public IntSumReducer() {
        }
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            IntWritable val = new IntWritable ();
            for(Iterator i$ = values.iterator(); i$.hasNext(); sum += val.get()) {
                val = (IntWritable)i$.next();
            }
            this.result.set(sum);
            context.write(key, this.result);
        }
    }
}

















