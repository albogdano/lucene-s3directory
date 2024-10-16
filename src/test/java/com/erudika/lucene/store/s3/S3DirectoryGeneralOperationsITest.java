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
import java.util.Arrays;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author kimchy
 */
public class S3DirectoryGeneralOperationsITest extends AbstractS3DirectoryITest {

	private static S3Directory s3Directory;

	public static final String TEST_BUCKET =  "TEST-lucene-s3-directory-dir";
	public static final String TEST_BUCKET1 = "TEST-lucene-s3-directory-dir1";
	public static final String TEST_BUCKET2 = "TEST-lucene-s3-directory-dir2";


	@BeforeClass
	public static void setUp() throws Exception {
		s3Directory = new S3Directory(TEST_BUCKET);
		s3Directory.create();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		s3Directory.close();
		s3Directory.delete();
	}

	@Test
	public void testList() throws IOException {
		assertTrue(s3Directory.bucketExists());

		assertFalse(s3Directory.fileExists("test1"));

		try (IndexOutput indexOutput = s3Directory.createOutput("test1", new IOContext(new FlushInfo(0, 0)))) {
			indexOutput.writeString("TEST STRING");
		}

		assertTrue(Arrays.asList(s3Directory.listAll()).contains("test1"));

		s3Directory.deleteFile("test1");

		assertFalse(s3Directory.fileExists("test1"));
	}

	@Test
	public void testDeleteContent() throws IOException {
		s3Directory.create();

		assertFalse(s3Directory.fileExists("test1"));

		try (IndexOutput indexOutput = s3Directory.createOutput("test1", new IOContext(new FlushInfo(0, 0)))) {
			indexOutput.writeString("TEST STRING");
		}

		assertTrue(Arrays.asList(s3Directory.listAll()).contains("test1"));

		s3Directory.emptyBucket();

		assertFalse(Arrays.asList(s3Directory.listAll()).contains("test1"));
	}
}
