package io.anserini.search;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.anserini.search.topicreader.TopicReader;
import io.anserini.search.topicreader.Topics;

public final class SearchHybrid <K extends Comparable<K>> {
  public static class Args extends HybridSearcher.Args {
    @Option(name = "-topics", metaVar = "[file]", handler = StringArrayOptionHandler.class, required = true, usage = "topics file")
    public String[] topics;

    @Option(name = "-output", metaVar = "[file]", required = true, usage = "output file")
    public String output;

    @Option(name = "-topicReader", usage = "TopicReader to use.")
    public String topicReader = "JsonIntVector";

    @Option(name = "-topicField", usage = "Topic field that should be used as the query.")
    public String topicField = "title";

    @Option(name = "-hits", metaVar = "[number]", usage = "max number of hits to return")
    public int hits = 1000;

    @Option(name = "-format", metaVar = "[output format]", usage = "Output format, default \"trec\", alternative \"msmarco\".")
    public String format = "trec";

    @Option(name = "-runtag", metaVar = "[tag]", usage = "runtag")
    public String runtag = "Anserini";
  }

  public Args args;
  public HybridSearcher<K> searcher;
  public List<K> qids;
  public List<String> queries;

  public SearchHybrid(Args args, HnswDenseSearcher.Args denseArgs, SearchCollection.Args sparseArgs) 
      throws IOException{
    this.args = args;
    this.searcher = new HybridSearcher<>(args, sparseArgs, denseArgs);
    
    // We might not be able to successfully read topics for a variety of reasons. Gather all possible
    // exceptions together as an unchecked exception to make initialization and error reporting clearer.
    SortedMap<K, Map<String, String>> topics = new TreeMap<>();
    for (String topicsFile : args.topics) {
      Path topicsFilePath = Paths.get(topicsFile);
      if (!Files.exists(topicsFilePath) || !Files.isRegularFile(topicsFilePath) || !Files.isReadable(topicsFilePath)) {
        Topics ref = Topics.getByName(topicsFile);
        if (ref==null) {
          throw new IllegalArgumentException(String.format("\"%s\" does not refer to valid topics.", topicsFilePath));
        } else {
          topics.putAll(TopicReader.getTopics(ref));
        }
      } else {
        try {
          @SuppressWarnings("unchecked")
          TopicReader<K> tr = (TopicReader<K>) Class
              .forName(String.format("io.anserini.search.topicreader.%sTopicReader", args.topicReader))
              .getConstructor(Path.class).newInstance(topicsFilePath);

          topics.putAll(tr.read());
        } catch (Exception e) {
          throw new IllegalArgumentException(String.format("Unable to load topic reader \"%s\".", args.topicReader));
        }
      }
    }
    // Now iterate through all the topics to pick out the right field with proper exception handling.
    try {
      topics.forEach((qid, topic) -> {
        String query;
        query = topic.get(args.topicField);
        assert query != null;
        qids.add(qid);
        queries.add(query);
      });
    } catch (AssertionError|Exception e) {
      throw new IllegalArgumentException(String.format("Unable to read topic field \"%s\".", args.topicField));
    }
  }

  public void run() {
    Map<K, ScoredDoc[]> results  =  searcher.batch_search(qids, queries,args.hits);
    try(RunOutputWriter<K> out = new RunOutputWriter<>(args.output, args.format, args.runtag, null)) {
      // zip query to results
      results.forEach((qid, hits) -> {
        try {
          out.writeTopic(qid, queries.get(qids.indexOf(qid)), results.get(qid));
        } catch (JsonProcessingException e) {
          // Handle the exception or rethrow as unchecked
          throw new RuntimeException(e);
        }
      });
    } catch (IOException e) {
      e.printStackTrace();
    }
 

  }

  public static void main(String[] args) {
    Args searchArgs = new Args();
    CmdLineParser searchParser = new CmdLineParser(searchArgs, ParserProperties.defaults().withUsageWidth(120));

    HnswDenseSearcher.Args denseArgs  = new HnswDenseSearcher.Args();
    CmdLineParser denseParser= new CmdLineParser(denseArgs, ParserProperties.defaults().withUsageWidth(120));
    
    SearchCollection.Args sparseArgs = new SearchCollection.Args();
    CmdLineParser sparseParser= new CmdLineParser(sparseArgs, ParserProperties.defaults().withUsageWidth(120));

    boolean inDenseArgs = false;
    boolean inSparseArgs = false;
    List<String> searchArgsList = new ArrayList<String>();
    List<String> denseArgsList = new ArrayList<String>();
    List<String> sparseArgsList = new ArrayList<String>();
    for (String arg : args) {
      if(arg.equals("dense")) {
        inDenseArgs = true;
        inSparseArgs = false;
      } else if (arg.equals("sparse")) {
        inDenseArgs = false;
        inSparseArgs = true;
      } else if (inSparseArgs) {
        sparseArgsList.add(arg);
      } else if (inDenseArgs) {
        denseArgsList.add(arg);
      } else {
        searchArgsList.add(arg);
      }
    }

    try {
      searchParser.parseArgument(searchArgsList);
      denseParser.parseArgument(denseArgsList);
      sparseParser.parseArgument(sparseArgsList);
    } catch (CmdLineException e) {
      System.out.println("ERROR");
    }

    try {
      SearchHybrid<?> search = new SearchHybrid<>(searchArgs, denseArgs, sparseArgs);
      search.run();
      // figure out what to do here
    } catch (Exception e) {
      System.err.printf("Error: %s\n", e.getMessage());
    }

  }

}
