package com.erudika.lucene.store.s3;

import static com.erudika.lucene.store.s3.S3DirectoryGeneralOperationsITest.TEST_BUCKET2;
import java.io.IOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class S3DirectoryIndexSearchITest extends AbstractS3DirectoryITest {

	private Directory directory;

	@Before
	public void setUp() throws Exception {
        directory = new S3Directory(TEST_BUCKET2);
        ((S3Directory) directory).create();
//		directory = FSDirectory.open(FileSystems.getDefault().getPath("target/index"));

		// create empty index
//        final IndexWriter iwriter = new IndexWriter(directory, getIndexWriterConfig());
//        iwriter.close();
	}

	@After
	public void tearDown() throws Exception {
        directory.close();
        ((S3Directory) directory).delete();
	}

	@Test
	public void testSearch() throws IOException, ParseException {
		// To store an index on disk, use this instead:
		// Directory directory = FSDirectory.open("/tmp/testindex");
		// final DirectoryTemplate template = new DirectoryTemplate(directory);
		// template.execute(new DirectoryTemplate.DirectoryCallbackWithoutResult() {
		// @Override
		// protected void doInDirectoryWithoutResult(final Directory dir) throws IOException {
		// try {
		// final DirectoryReader ireader = DirectoryReader.open(directory);
		// final IndexSearcher isearcher = new IndexSearcher(ireader);
		// // Parse a simple query that searches for "text":
		//
		// final QueryParser parser = new QueryParser("fieldname", analyzer);
		// final Query query = parser.parse("text");
		// final ScoreDoc[] hits = isearcher.search(query, null, 1000).scoreDocs;
		// Assert.assertEquals(0, hits.length);
		// ireader.close();
		// } catch (final ParseException e) {
		// throw new IOException(e);
		// }
		// }
		//
		// });
		//
		// template.execute(new DirectoryTemplate.DirectoryCallbackWithoutResult() {
		// @Override
		// public void doInDirectoryWithoutResult(final Directory dir) throws IOException {
		// final IndexWriter iwriter = new IndexWriter(directory, getIndexWriterConfig());
		// final Document doc = new Document();
		// final String text = "This is the text to be indexed.";
		// doc.add(new Field("fieldname", text, TextField.TYPE_STORED));
		// iwriter.addDocument(doc);
		// iwriter.close();
		// }
		// });
		try (IndexWriter iwriter = new IndexWriter(directory, getIndexWriterConfig())) {
			final Document doc = new Document();
			final String text = "This is the text to be indexed.";
			doc.add(new Field("fieldname", text, TextField.TYPE_STORED));
			iwriter.addDocument(doc);
			if (iwriter.hasUncommittedChanges()) {
				iwriter.commit();
			}	if (iwriter.isOpen()) {
				iwriter.getDirectory().close();
			}	iwriter.forceMerge(1, true);
		}
		// Now search the index:
		try (DirectoryReader ireader = DirectoryReader.open(directory)) {
			final IndexSearcher isearcher = new IndexSearcher(ireader);
			// Parse a simple query that searches for "text":

			final QueryParser parser = new QueryParser("fieldname", analyzer);
			final Query query = parser.parse("text");
			final ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;
			Assert.assertEquals(1, hits.length);
			// Iterate through the results:
			for (final ScoreDoc hit : hits) {
				final Document hitDoc = isearcher.doc(hit.doc);
				Assert.assertEquals("This is the text to be indexed.", hitDoc.get("fieldname"));
			}
			ireader.close();
		} catch (final ParseException e) {
			throw new IOException(e);
		}
		try (DirectoryReader ireader = DirectoryReader.open(directory)) {
			final IndexSearcher isearcher = new IndexSearcher(ireader);
			// Parse a simple query that searches for "text":

			final QueryParser parser = new QueryParser("fieldname", analyzer);
			final Query query = parser.parse("text");
			final ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;
			Assert.assertEquals(1, hits.length);
			// Iterate through the results:
			for (final ScoreDoc hit : hits) {
				final Document hitDoc = isearcher.doc(hit.doc);
				Assert.assertEquals("This is the text to be indexed.",
						hitDoc.get("fieldname"));
			}
		}
		// Parse a simple query that searches for "text":
	}

	private IndexWriterConfig getIndexWriterConfig() {
		final IndexWriterConfig config = new IndexWriterConfig(analyzer);
		config.setOpenMode(OpenMode.CREATE_OR_APPEND);
		return config;
	}
}
