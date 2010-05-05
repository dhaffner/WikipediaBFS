import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.mapred.NoSplitSequenceFileInputFormat;
import edu.umd.cloud9.util.FSLineReader;

public class WikipediaBFS extends Configured implements Tool 
{
    private static final int NUM_MAPS = 100; // Number of maps to use...
    private static final int NUM_REDUCES = 30;
    private static final int ITERATIONS = 60; // BFS iterations to run

    private static final int WHITE = 0; // Node colors, per the algorithm
    private static final int GRAY = 1;
    private static final int BLACK = 3;

    public WikipediaBFS() 
    {
    }
    
    // Take the input SequenceFiles and extract each page's links
    // Emit something like <Page, [Link1, Link2, ...]> plus other stuff to run the BFS with.
    private static class LinkExtractMap implements
    MapRunnable<IntWritable, WikipediaPage, Text, Text> 
    {   
        private static final Text sOutputKey = new Text();
        private static final Text sOutputValue = new Text();

        private static final String source = "paul erdős"; //"kevin bacon"; // "greg kesden";

        @SuppressWarnings("unchecked")
        public void configure(JobConf job) 
        {
        }

        public void run(RecordReader<IntWritable, WikipediaPage> input,
            OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
        {

            IntWritable key = new IntWritable();
            WikipediaPage value = new WikipediaPage();

            String text;
            String id;

            while (input.next(key, value)) {
                WikipediaPage.readPage(value, value.getRawXML()); // Parse the page

                // Skip 'non-articles'
                if (!isUsefulPage(value))
                    continue; 

                text = value.getText();

                // Extract all links from this page's text
                HashSet<String> links = extractLinks(value);

                // Skip isolated vertices
                if (links.isEmpty())
                    continue;

                id = normalizeTitle(value.getTitle());

                /*  From the blog post on BFS:

                ID    EDGES|DISTANCE_FROM_SOURCE|COLOR|

                    where EDGES is a comma delimited list of the ids of the nodes that
                    are connected to this node. in the beginning, we do not know the 
                    distance and will use Integer.MAX_VALUE for marking "unknown". the 
                    color tells us whether or not we've seen the node before, so this 
                    starts off as white.
                    suppose we start with the following input graph, in which we've 
                    stated that node #1 is the source (starting point) for the search, 
                    and as such have marked this one special node with distance 0 and 
                    color GRAY.


                    in our case..
                    ID = this page's title
                    EDGES = links
                    DISTANCE_FROM_SOURCE = as described
                    COLOR = as described
                */

                StringBuffer BFSValue = new StringBuffer();
                for (String neighbor : links)
                {
                    // From the Wikipedia title guidelines, we know titles can't contain #
                    if (hasSkippablePrefix(neighbor))
                        continue;

                    // Using # to separate links since the Wikipedia title guidelines state
                    // that titles can't have the # char, among others
                    BFSValue.append(normalizeTitle(neighbor) + "#");
                }

                BFSValue.append("|");

                if (id.equals(source)) // This node is the "kevin bacon" node
                {
                    reporter.incrCounter(BFSCounters.FOUND_SOURCE, 1);
                    BFSValue.append(0);
                    BFSValue.append("|");
                    BFSValue.append(GRAY);
                }

                else
                {
                    BFSValue.append(Integer.MAX_VALUE); // Default DISTANCE_FROM_SOURCE
                    BFSValue.append("|");
                    BFSValue.append(WHITE);    
                }

                sOutputKey.set(id);
                sOutputValue.set(BFSValue.toString());
                output.collect(sOutputKey, sOutputValue);
            }
        }

        // To avoid case-inconsistencies among articles and linking, we'll make everything
        // lowercase and remove any | chars
        private String normalizeTitle(String title)
        {
            return title.toLowerCase().replaceAll("\\|", "");
        }

        // Extract all links from the given page, return them as a HashSet
        // (Using a HashSet to eliminate duplicates.)
        private HashSet<String> extractLinks(WikipediaPage page)
        {
            HashSet<String> set = new HashSet<String>();

            String text = page.getText();
            String linkText;

            int linkStart = 0;
            int linkEnd = 0;
            int pipe = 0;

            while (true)
            {
                // Links on Wikipedia are of the form [[page_name]] or [[page_name|Display text]]
                linkStart = text.indexOf("[[", linkEnd);
                if (linkStart < 0) // No links in this text
                    break;

                linkEnd = text.indexOf("]]", linkStart);
                if (linkEnd < 0 || (linkEnd-linkStart) <= 2) // Link started but didn't end,
                    break;                                   // or, link was "[[]]" 

                linkText = text.substring(linkStart+2, linkEnd);

                pipe = linkText.indexOf("|");
                if (pipe > 0)
                    linkText = linkText.substring(0, pipe);

                set.add(linkText);
            }

            return set;
        }

        private boolean isUsefulPage(WikipediaPage page)
        {
            // Possibly add page.isDisambiguation()?
            return !(page.isRedirect() || page.isEmpty() || page.isStub() || hasSkippablePrefix(page.getTitle()));
        }

        private boolean hasSkippablePrefix(String title)
        {
            String[] ignorePrefixes = {"File:", "Category:", "Portal:", "Wikipedia:", "Image:"};
            for (String prefix : ignorePrefixes)
                if (title.startsWith(prefix))
                return true;

            return false;
        }
    }

    // Runs one pass of the BFS algorithm as described in the blog post linked from the lab handout.
    // http://johnandcailin.com/blog/cailin/breadth-first-graph-search-using-iterative-map-reduce-algorithm
    private static class BFSMap implements
        MapRunnable<Text, Text, Text, Text> 
    {
        @SuppressWarnings("unchecked")
        public void configure(JobConf job) 
        {
        }

        public void run(RecordReader<Text, Text> input,
            OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
        {
            Text key = new Text();
            Text value = new Text();

            String text;
            String id;
            String page;
            String links;
            String[] tokens;

            int distance;
            int color;
            int p1, p2;

            // Iterate through each page (key)
            while (input.next(key, value)) {
                page = key.toString();
                text = value.toString(); // This is "link1#link2#link3...|distance|color"
                
                tokens = text.split("\\|");
                if (tokens.length < 3) // Malformed record
                    continue;
                
                links = tokens[0];
                distance = Integer.parseInt(tokens[1]);
                color = Integer.parseInt(tokens[2]);

                if (color == GRAY)
                {
                    for (String link : links.split("\\#"))
                    {
                        if (link.length() > 0)
                            output.collect(new Text(link), new Text(formValueString(null, distance+1, GRAY)));
                    }
                    color = BLACK;
                }

                output.collect(key, new Text(formValueString(links, distance, color)));
            }
        }
    } 

    // Reduce each node into one node as described below.
    // 2nd part of a single BFS iteration.
    public static class BFSReduce extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> 
    {

        /**
            * Make a new node which combines all information for this single node id.
            * The new node should have 
            * - The full list of edges 
            * - The minimum distance 
            * - The darkest Color
            */
        public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
        {

            String text;
            String links = "";
            String[] tokens;
            
            int distance = Integer.MAX_VALUE, d;
            int color = WHITE, c;

            while (values.hasNext()) 
            {                
                text = values.next().toString();
                
                tokens = text.split("\\|");
                if (tokens.length < 3) // Malformed record
                    continue;

                if (tokens[0].length() > 0)
                    links = tokens[0];

                d = Integer.parseInt(tokens[1]);                
                if (d < distance) // Take the minimum distance...
                    distance = d;

                c = Integer.parseInt(tokens[2]);
                if (c > color) // ...and the 'darkest' color
                    color = c;
            }
            
            if (color == GRAY)
            {
                // This emulates the "keepGoing" boolean described in the blog post.
                // Although I didn't rely on this completely, it was useful to include
                // since counters are straightforward and very cool.
                reporter.incrCounter(BFSCounters.KEEP_GOING, 1);
            }
            
            // Output a node with the links, the minimum distance, and darkest color
            output.collect(key, new Text(formValueString(links, distance, color)));
        }
    }
    
    // Tally counts for the distance ranges
    // i.e., incremenets counters for distances observed in a finite amount of ranges.
    // Used in the write up for numbers analyses.
    public static class BFSDistanceFilterReduce extends MapReduceBase implements
        Reducer<Text, Text, Text, Text> 
    {
        
        //private static int LIMIT = 10;

        /**
            * Make a new node which combines all information for this single node id.
            * The new node should have 
            * - The full list of edges 
            * - The minimum distance 
            * - The darkest Color
            */
        public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
        {

            String text;
            String links = "";
            String[] tokens;
            
            int distance = Integer.MAX_VALUE, d;

            while (values.hasNext()) 
            {                
                text = values.next().toString();
                
                tokens = text.split("\\|");
                if (tokens.length < 3) // Malformed record
                    continue;

                d = Integer.parseInt(tokens[1]);                
                if (d < distance) // Take the minimum distance...
                    distance = d;
            }
            
            reporter.incrCounter(BFSCounters.NUM_RECORDS, 1);
            if (distance <= 5)
            {
                reporter.incrCounter(BFSCounters.NUM_5, 1);
                output.collect(key, new Text("" + distance));
            }
            else if (distance <= 10)
                reporter.incrCounter(BFSCounters.NUM_10, 1);
            else if (distance <= 20)
                reporter.incrCounter(BFSCounters.NUM_20, 1);                            
            else if (distance <= 30)
                reporter.incrCounter(BFSCounters.NUM_30, 1);
            else if (distance <= 40)
                reporter.incrCounter(BFSCounters.NUM_40, 1);                
            else
                reporter.incrCounter(BFSCounters.NUM_OVER_40, 1);

        }
    }
    
    private static String formValueString(String links, int distance, int color)
    {
        StringBuffer value = new StringBuffer();

        if (links != null)
            value.append(links);

        value.append("|");
        value.append(distance);
        value.append("|");
        value.append(color);

        return value.toString();
    }

    // Using different functions to get JobConf's for
    // each phase to keep the run function not too long

    private JobConf FilterIteration()
    {
        JobConf conf = new JobConf(getConf(), WikipediaBFS.class);

        conf.setJobName("WikipediaBFS-BFSFilter");

        conf.setInputFormat(KeyValueTextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(IdentityMapper.class);
        conf.setReducerClass(BFSDistanceFilterReduce.class);

        conf.setNumMapTasks(NUM_MAPS);
        conf.setNumReduceTasks(NUM_REDUCES);

        return conf;        
    }

    private JobConf BFSIteration() 
    {
        JobConf conf = new JobConf(getConf(), WikipediaBFS.class);

        conf.setJobName("WikipediaBFS-BFSIteration");

        conf.setInputFormat(KeyValueTextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapRunnerClass(BFSMap.class);
        conf.setReducerClass(BFSReduce.class);

        conf.setNumMapTasks(NUM_MAPS);
        conf.setNumReduceTasks(NUM_REDUCES);

        return conf;
    }

    private JobConf firstPass()
    {
        JobConf conf = new JobConf(WikipediaBFS.class);

        conf.setJobName("WikipediaBFS-FirstPass");
        
        conf.setInputFormat(NoSplitSequenceFileInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);        

        conf.setMapRunnerClass(LinkExtractMap.class);
        conf.setReducerClass(IdentityReducer.class);
        
        conf.setNumMapTasks(NUM_MAPS);
        conf.setNumReduceTasks(NUM_REDUCES);
        
        return conf;
    }

    private static int printUsage() 
    {
        System.out.println("usage: [collection-path] [output-path]");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }
    
    protected static enum BFSCounters
    {
        FOUND_SOURCE,
        KEEP_GOING,
        NUM_5,
        NUM_10,
        NUM_20,
        NUM_30,
        NUM_40,
        NUM_OVER_40,
        NUM_RECORDS
    };

    /**
        * Runs this tool.
        */
    public int run(String[] args) throws Exception 
    {
        System.out.println(Arrays.toString(args));
        
        if (args.length != 2) 
        {
            printUsage();
            return -1;
        }

        int iteration = 0;
        
        String input = args[0]; // the Wikipedia SequenceFiles
        String outputBase = args[1]; // the base directory the initial input for our BFS goes
        String output = String.format("%sBFS-%d", outputBase, iteration);

        JobConf conf = firstPass();
        FileSystem fs = FileSystem.get(conf);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));
        FileOutputFormat.setCompressOutput(conf, false);
        
        RunningJob job;
        long found_source;
        
        // First pass
        fs.delete(new Path(output), true); // delete the output directory if it exists already
        job = JobClient.runJob(conf);
        
        found_source = job.getCounters().getCounter(BFSCounters.FOUND_SOURCE);
        if (found_source > 0)
        {
            System.out.println("Found source node...\n");
            System.out.printf("Running %d BFS iterations\n", ITERATIONS);
            while (iteration++ < ITERATIONS)
            {
                conf = BFSIteration();
                
                if (iteration > 1)
                {
                    System.out.printf("Deleting old input %s\n", input);
                    fs.delete(new Path(input), true);
                }
                input = output;
                output = String.format("%sBFS-%d", outputBase, iteration);
                System.out.printf("BFS iteration %d: %s -> %s\n", iteration, input, output);

                fs.delete(new Path(output), true);
                FileInputFormat.setInputPaths(conf, new Path(input));
                FileOutputFormat.setOutputPath(conf, new Path(output));
                job = JobClient.runJob(conf);
    
                Counters c = job.getCounters();
                long keepGoing = c.getCounter(BFSCounters.KEEP_GOING);
                if (keepGoing == 0)
                {
                    System.out.println("Should end BFS here...");
                    break;
                } 
            }
        }
        else {
            System.out.println("Didn't find source node.\n");
            return 0;
        }

        conf = FilterIteration();
        input = output; // Use the output from the last BFS iteration
        output = String.format("%sBFS-FILTER", outputBase);
        System.out.printf("BFS range filter: %s -> %s\n", input, output);
        
        // Output will be all keys within a distance of 5
        fs.delete(new Path(output), true);
        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));
        job = JobClient.runJob(conf);
        
        // Not outputting the counters' values since Hadoop does it by default.  

        return 0;
    }

    /**
        * Dispatches command-line arguments to the tool via the
        * <code>ToolRunner</code>.
        */
    public static void main(String[] args) throws Exception 
    {
        int res = ToolRunner.run(new Configuration(), new WikipediaBFS(), args);
        System.exit(res);
    }
}