/*
 * Copyright 2013-2025 Erudika. http://erudika.com
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
package com.erudika.lucene.store.s3;

import com.adobe.testing.s3mock.S3MockApplication;
import com.github.davidmoten.aws.lw.client.Client;
import com.github.davidmoten.aws.lw.client.Credentials;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.MMapDirectory;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class S3DirectoryTest { //extends BaseDirectoryTestCase {

	public static final String TEST_BUCKET = "TEST-lucene-s3-directory-dir";
	public static final String TEST_BUCKET1 = "TEST-lucene-s3-directory-dir1";

	private static S3MockApplication s3Mock;
	private static Client s3;
	private static S3Directory s3Directory;

	private static S3Directory s3Directory2;
	private static Directory fsDirectory;
	private static Directory ramDirectory;
//	private final Analyzer analyzer = new MockAnalyzer(random());

	private final Collection<String> docs = loadDocuments(3000, 5);
	private final OpenMode openMode = OpenMode.CREATE_OR_APPEND;
	private final boolean useCompoundFile = false;

	@BeforeAll
	public static void setUpClass() throws Exception {
		//System.setProperty("logging.level.com.erudika.lucene.*", "DEBUG");
		Map<String, Object> props = new HashMap<>();
		props.put(S3MockApplication.PROP_SILENT, "true");
		s3Mock = S3MockApplication.start(props);
		s3 = Client.s3()
						.region("us-east-1")
						.credentials(Credentials.of("foo", "bar"))
						.baseUrlFactory((serviceName, region) -> "http://localhost:9090/")
						.build();
		s3Directory = new S3Directory(s3, TEST_BUCKET, "");
		s3Directory.create();

		ramDirectory = new MMapDirectory(FileSystems.getDefault().getPath("target/index"));
		fsDirectory = FSDirectory.open(FileSystems.getDefault().getPath("target/index"));

		s3Directory2 = new S3Directory(s3, TEST_BUCKET1, "");
	}

	@AfterAll
	public static void tearDownClass() throws Exception {
		s3Directory.emptyBucket();
		s3Directory.close();
		s3Directory.delete();
		s3Directory2.close();
		s3Directory2.delete();
		ramDirectory.close();
		fsDirectory.close();
		s3Mock.stop();
		s3Mock = null;
		s3Directory = null;
		s3Directory2 = null;
		ramDirectory = null;
		fsDirectory = null;
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

	protected void addDocuments(
			final Directory directory,
			final IndexWriterConfig.OpenMode openMode,
			final boolean useCompoundFile,
			final Collection<String> docs)
			throws IOException {
		final IndexWriterConfig config = new IndexWriterConfig();
		config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
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
			// writer.optimize();
		}
	}

	private long timeIndexWriter(final Directory dir) throws IOException {
		final long start = System.nanoTime();
		addDocuments(dir, openMode, useCompoundFile, docs);
		final long stop = System.nanoTime();
		return stop - start;
	}

	private IndexWriterConfig getIndexWriterConfig() {
		final IndexWriterConfig config = new IndexWriterConfig();
		config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
		return config;
	}

	@Test
	public void testSearch() throws IOException {
		try (IndexWriter iwriter = new IndexWriter(s3Directory, getIndexWriterConfig())) {
			final Document doc = new Document();
			final String text = "This is the text to be indexed.";
			doc.add(new Field("fieldname", text, TextField.TYPE_STORED));
			iwriter.addDocument(doc);
			if (iwriter.hasUncommittedChanges()) {
				iwriter.commit();
			}
			// if (iwriter.isOpen()) {
			//   iwriter.getDirectory().close();
			// }
			// iwriter.forceMerge(1, true);
		}
		// Now search the index:
		final Query query = new TermQuery(new Term("fieldname", "text"));
		try (DirectoryReader ireader = DirectoryReader.open(s3Directory)) {
			final IndexSearcher isearcher = new IndexSearcher(ireader);
			final TopDocs topDocs = isearcher.search(query, 1000);
			final StoredFields storedFields = isearcher.storedFields();
			final ScoreDoc[] hits = topDocs.scoreDocs;
			assertEquals(1, hits.length);
			// Iterate through the results:
			for (final ScoreDoc hit : hits) {
				final Document hitDoc = storedFields.document(hit.doc);
				assertEquals("This is the text to be indexed.", hitDoc.get("fieldname"));
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
		try (DirectoryReader ireader = DirectoryReader.open(s3Directory)) {
			final IndexSearcher isearcher = new IndexSearcher(ireader);
			final TopDocs topDocs = isearcher.search(query, 1000);
			final StoredFields storedFields = isearcher.storedFields();
			final ScoreDoc[] hits = topDocs.scoreDocs;
			assertEquals(1, hits.length);
			// Iterate through the results:
			for (final ScoreDoc hit : hits) {
				final Document hitDoc = storedFields.document(hit.doc);
				assertEquals("This is the text to be indexed.", hitDoc.get("fieldname"));
			}
		}
	}

	@Test
	public void testList() throws IOException {
		assertTrue(s3Directory.bucketExists());

		assertFalse(s3Directory.fileExists("test1"));

		try (IndexOutput indexOutput = s3Directory.createOutput("test1", null)) {
			indexOutput.writeString("TEST STRING");
		}

		assertTrue(Arrays.asList(s3Directory.listAll()).contains("test1"));

		s3Directory.deleteFile("test1");

		assertFalse(s3Directory.fileExists("test1"));
	}

	@Test
	public void testDeleteContent() throws IOException, InterruptedException {
		s3Directory.create();

		assertFalse(s3Directory.fileExists("test1"));

		try (IndexOutput indexOutput = s3Directory.createOutput("test1", null)) {
			indexOutput.writeString("TEST STRING");
		}

		assertTrue(Arrays.asList(s3Directory.listAll()).contains("test1"));

		s3Directory.emptyBucket();

		assertFalse(Arrays.asList(s3Directory.listAll()).contains("test1"));
	}

	@Test
	public void testTiming() throws IOException {
		final long ramTiming = timeIndexWriter(ramDirectory);
		final long fsTiming = timeIndexWriter(fsDirectory);
		final long s3Timing = timeIndexWriter(s3Directory);

		System.out.println("RAMDirectory Time: " + ramTiming + " ms");
		System.out.println("FSDirectory Time : " + fsTiming + " ms");
		System.out.println("S3Directory Time : " + s3Timing + " ms");
	}

	@Test
	public void testSize5() throws IOException {
		innerTestSize(5);
	}

	@Test
	public void testSize5WithinTransaction() throws IOException {
		innertTestSizeWithinTransaction(5);
	}

	@Test
	public void testSize15() throws IOException {
		innerTestSize(15);
	}

	@Test
	public void testSize15WithinTransaction() throws IOException {
		innertTestSizeWithinTransaction(15);
	}

	@Test
	public void testSize2() throws IOException {
		innerTestSize(2);
	}

	@Test
	public void testSize2WithinTransaction() throws IOException {
		innertTestSizeWithinTransaction(2);
	}

	@Test
	public void testSize1() throws IOException {
		innerTestSize(1);
	}

	@Test
	public void testSize1WithinTransaction() throws IOException {
		innertTestSizeWithinTransaction(1);
	}

	@Test
	public void testSize50() throws IOException {
		innerTestSize(50);
	}

	@Test
	public void testSize50WithinTransaction() throws IOException {
		innertTestSizeWithinTransaction(50);
	}

	@Test
	public void testLocks() throws Exception {
		try (Lock lock1 = s3Directory.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
			lock1.ensureValid();
			try {
				s3Directory2.obtainLock(IndexWriter.WRITE_LOCK_NAME);
				fail("lock2 should not have valid lock");
			} catch (final Exception e) {
				e.getCause();
			}
		}
	}

	private void innerTestSize(final int bufferSize) throws IOException {
		insertData();
		verifyData();
	}

	private void innertTestSizeWithinTransaction(final int bufferSize) throws IOException {
		insertData();
		verifyData();
	}

	private void insertData() throws IOException {
		if (s3Directory.fileExists("value1")) {
			s3Directory.deleteFile("value1");
		}
		final byte[] test = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
		try (IndexOutput indexOutput
				= s3Directory.createOutput("value1", new IOContext(new FlushInfo(0, 0)))) {
			indexOutput.writeInt(-1);
			indexOutput.writeLong(10);
			indexOutput.writeInt(0);
			indexOutput.writeInt(0);
			indexOutput.writeBytes(test, 8);
			indexOutput.writeBytes(test, 5);
			indexOutput.writeByte((byte) 8);
			indexOutput.writeBytes(new byte[]{1, 2}, 2);
		}
	}

	private void verifyData() throws IOException {
		final byte[] test = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
		assertTrue(s3Directory.fileExists("value1"));
		assertEquals(36, s3Directory.fileLength("value1"));

		try (IndexInput indexInput
				= s3Directory.openInput("value1", new IOContext(new FlushInfo(0, 0)))) {
			assertEquals(-1, indexInput.readInt());
			assertEquals(10, indexInput.readLong());
			assertEquals(0, indexInput.readInt());
			assertEquals(0, indexInput.readInt());
			indexInput.readBytes(test, 0, 8);
			assertEquals((byte) 1, test[0]);
			assertEquals((byte) 8, test[7]);
			indexInput.readBytes(test, 0, 5);
			assertEquals((byte) 1, test[0]);
			assertEquals((byte) 5, test[4]);

			indexInput.seek(28);
			assertEquals((byte) 1, indexInput.readByte());
			indexInput.seek(30);
			assertEquals((byte) 3, indexInput.readByte());
		}
	}

	protected Directory getDirectory(Path path) throws IOException {
		S3Directory dir = new S3Directory(s3, TEST_BUCKET + "-" + path.getFileName(), "");
		try {
			dir.create();
		} catch (InterruptedException ex) {
			throw new RuntimeException(ex);
		}
		return dir;
	}
}
