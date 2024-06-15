package io.anserini.search;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommands;

public class HybridSearcher <K extends Comparable<K>>{
  protected final Args args;
  protected final SearchCollection.Args sparseArgs;

  public static class Args {
    @Option(name = "-alpha", metaVar = "[double]", required = false, usage = "weight of BM25")
    public double alpha = 1.1; // TODO: Verify initial value
  }

  private SimpleSearcher sparseSearcher;
  private HnswDenseSearcher<K> denseSearcher;

  // private List<String> queries;
  // private List<String> qids;

  public Map<K, ScoredDoc[]> batch_search(List<K> qids, List<String>queries, int k) {
    Map<String, ScoredDoc[]> sparseResults =  sparseSearcher.batch_search(
      qids.stream().map(i ->i.toString()).collect(Collectors.toList()), 
      queries, k, sparseArgs.threads);

    // threads is set on initialization
    Map<K, ScoredDoc[]> denseResults = denseSearcher.batch_search(qids, queries, k); 


    // Fusion matches implementation from https://link.springer.com/chapter/10.1007/978-3-030-99736-6_41
    Map<K, ScoredDoc[]> finalResults = new TreeMap<K, ScoredDoc[]>();
    for (K qid: qids){
      List<ScoredDoc> fusedResult = Arrays.asList(sparseResults.get(qid.toString()));
      List<ScoredDoc> denseResult = Arrays.asList(denseResults.get(qid));

      float minSparse = Collections.min(fusedResult, new ScoredDoc.ScoredDocScoreComparator()).score;
      float minDense = Collections.min(denseResult, new ScoredDoc.ScoredDocScoreComparator()).score;

      for (ScoredDoc d : fusedResult) {
        Iterator<ScoredDoc> it = denseResult.iterator();
        boolean found = false;
        while(it.hasNext()){
          ScoredDoc denseDoc = it.next();
          if (denseDoc.docid.equals(d.docid)) {
            d.score += (d.score) * args.alpha + denseDoc.score;
            it.remove();
            found = true;
            break;
          }
        }
        if(!found) {
          d.score = minDense;
        }
      }
      // all elements left in dense are not in sparse
      for(ScoredDoc d : denseResult) {
        d.score = minSparse; // should this be scaled by alpha
        fusedResult.add(d);
      }
      fusedResult.sort(new ScoredDoc.ScoredDocScoreComparator());
      finalResults.put(qid, fusedResult.subList(0,k).toArray(new ScoredDoc[k]));
    }

    
    return finalResults;
  }

	public HybridSearcher(Args args, SearchCollection.Args sparseArgs, HnswDenseSearcher.Args denseArgs) 
      throws IOException {
    this.args = args;
    this.sparseArgs = sparseArgs;
    this.sparseSearcher = new SimpleSearcher(sparseArgs.index);
    this.denseSearcher = new HnswDenseSearcher<K>(denseArgs);
	}
}
