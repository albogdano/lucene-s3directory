/*
 * Copyright 2004-2019 the original author or authors.
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
 */
package com.erudika.lucene.store.s3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * @author kimchy
 */
public abstract class AbstractS3DirectoryITest {

	protected Analyzer analyzer = new SimpleAnalyzer();

	@BeforeClass
	public static void initDatabase() throws IOException {
	}

	@AfterClass
	public static void closeDatabase() {
	}

	@Before
	public void initAttributes() throws Exception {
	}

	protected Collection<String> loadDocuments(final int numDocs, final int wordsPerDoc) {
		final Collection<String> docs = new ArrayList<String>(numDocs);
		for (int i = 0; i < numDocs; i++) {
			final StringBuffer doc = new StringBuffer(wordsPerDoc);
			for (int j = 0; j < wordsPerDoc; j++) {
				doc.append("Bibamus ");
			}
			docs.add(doc.toString());
		}
		return docs;
	}

	protected void addDocuments(final Directory directory, final OpenMode openMode, final boolean useCompoundFile,
			final Collection<String> docs) throws IOException {
		final IndexWriterConfig config = new IndexWriterConfig(analyzer);
		config.setOpenMode(OpenMode.CREATE);
		config.setUseCompoundFile(useCompoundFile);

		try (IndexWriter writer = new IndexWriter(directory, config)) {
			for (final Object element : docs) {
				final Document doc = new Document();
				final String word = (String) element;
				doc.add(new StringField("keyword", word, Field.Store.YES));
				doc.add(new StringField("unindexed", word, Field.Store.YES));
				doc.add(new StringField("unstored", word, Field.Store.NO));
				doc.add(new StringField("text", word, Field.Store.YES));
				writer.addDocument(doc);
			}
			// FIXME: review
			// writer.optimize();
		} catch (Exception e) {

		}
	}
}
