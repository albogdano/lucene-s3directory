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

import static com.erudika.lucene.store.s3.S3DirectoryGeneralOperationsITest.bucketName;
import static com.erudika.lucene.store.s3.S3DirectoryGeneralOperationsITest.path;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.Collection;

import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author kimchy
 */
public class S3DirectoryBenchmarkITest extends AbstractS3DirectoryITest {

	private static Directory fsDirectory;
	private static Directory ramDirectory;
	private static Directory s3Directory;

	private final Collection<String> docs = loadDocuments(3000, 5);
	private final OpenMode openMode = OpenMode.CREATE_OR_APPEND;
	private final boolean useCompoundFile = false;

	@BeforeClass
	public static void setUp() throws Exception {
		s3Directory = new S3Directory(bucketName, path);
		((S3Directory) s3Directory).create();
		ramDirectory = new MMapDirectory(FileSystems.getDefault().getPath("target/index"));
		fsDirectory = FSDirectory.open(FileSystems.getDefault().getPath("target/index"));
	}

	@AfterClass
	public static void tearDown() throws Exception {
		s3Directory.close();
		((S3Directory) s3Directory).delete();
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

	private long timeIndexWriter(final Directory dir) throws IOException {
		final long start = System.currentTimeMillis();
		addDocuments(dir, openMode, useCompoundFile, docs);
		final long stop = System.currentTimeMillis();
		return stop - start;
	}

}
