/*
 * Copyright 2013-2022 Erudika. http://erudika.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For issues and patches go to: https://github.com/erudika
 */
package com.erudika.lucene.store.s3.index;

import com.erudika.lucene.store.s3.S3Directory;
import java.io.IOException;
import java.text.ParseException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

public class ReadIndex {

	public static void main(String[] args) throws IOException, ParseException, org.apache.lucene.queryparser.classic.ParseException {
		S3Directory s3Directory = new S3Directory("lucene-test", "lucene-kashyap");
		try ( IndexReader indexReader = DirectoryReader.open(s3Directory)) {
			IndexSearcher searcher = new IndexSearcher(indexReader);

			QueryParser queryParser = new QueryParser("CONTENT", new StandardAnalyzer());
			Query parseredQuery = queryParser.parse("oracle");
			TopDocs result = searcher.search(parseredQuery, 10000);
			StoredFields storedFields = searcher.storedFields();
			System.out.println(result.scoreDocs.length);
			for (ScoreDoc scoreDoc : result.scoreDocs) {
				final Document document = storedFields.document(scoreDoc.doc);
				final String documentId = document.get("ID");
				final String table = document.get("TABLE");
				System.out.println(table + " " + documentId + " " +  scoreDoc.score);
			}
		}
	}
}
