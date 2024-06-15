/*
 * Anserini: A Lucene toolkit for reproducible information retrieval research
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.anserini.search;

import java.util.Comparator;

import org.apache.lucene.document.Document;

/**
 * See documentation for {@link ScoredDocs}.
 */
public class ScoredDoc {
  public String docid;
  public int lucene_docid;
  public float score;
  public Document lucene_document;

  public ScoredDoc(String docid, int lucene_docid, float score, Document lucene_document) {
    this.docid = docid;
    this.lucene_docid = lucene_docid;
    this.score = score;
    this.lucene_document = lucene_document;
  }

  public static class ScoredDocScoreComparator implements Comparator<ScoredDoc>{
    @Override
    public int compare(ScoredDoc o1, ScoredDoc o2) {
      if(o1.score > o2.score) {
        return 1;
      } if (o2.score < o1.score) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  public class ScoredDocIdComparator implements Comparator<ScoredDoc> {
    @Override
    public int compare(ScoredDoc o1, ScoredDoc o2) {
      return o1.docid.compareTo(o2.docid); 
    }
  }
}

