import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        //TODO
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] sourceAndTargets = value.toString().split(": ", 2);
            Integer source = Integer.parseInt(sourceAndTargets[0]);
            String targetsStr = sourceAndTargets[1];
            String[] targets = targetsStr.split(" ");

            context.write(new IntWritable(source), new IntWritable(0));
            for (String targetStr : targets) {
                Integer target = Integer.parseInt(targetStr);
                if (!target.equals(source)) {
                    context.write(new IntWritable(target), new IntWritable(1));
                }
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }

            context.write(key, new IntWritable(count));
        }
    }

    public static class LeagueMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
        private TreeSet<Integer> league = new TreeSet<Integer>();

        private TreeSet<Pair<Integer, Integer>> countToWordMap = new TreeSet<Pair<Integer, Integer>>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();

            String leaguePath = conf.get("league");

            List<String> leaguesStr = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"))
            for (String leagueStr : leaguesStr) {
                league.add(Integer.parseInt(leagueStr));
            }
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer target = Integer.parseInt(key.toString());
            Integer count = Integer.parseInt(value.toString());

            if (league.contains(target)) {
                countToWordMap.add(new Pair<Integer, Integer>(count, target));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Integer, Integer> item : countToWordMap) {
                Integer[] integers = { item.second, item.first };
                IntArrayWritable val = new IntArrayWritable(integers);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class LeagueReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        private TreeSet<Pair<Integer, Integer>> countToWordMap = new TreeSet<Pair<Integer, Integer>>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
        }
        
        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntArrayWritable val : values) {
                IntWritable[] pair = (IntWritable[]) val.toArray();
                Integer target = pair[0].get();
                Integer count = pair[1].get();

                countToWordMap.add(new Pair<Integer, Integer>(count, target));
            }

            for (Pair<Integer, Integer> item : countToWordMap) {
                IntWritable target = new IntWritable(item.second);
                IntWritable value = new IntWritable(item.first);
                context.write(target, value);
            }
        }
    }
}

class Pair<A extends Comparable<? super A>, B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>, B extends Comparable<? super B>> Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
