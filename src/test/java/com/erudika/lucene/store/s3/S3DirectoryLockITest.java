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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Lock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author kimchy
 */
public class S3DirectoryLockITest extends AbstractS3DirectoryITest {

	private S3Directory dir1;
	private S3Directory dir2;

	@Before
	public void setUp() throws Exception {
		final S3DirectorySettings settings = new S3DirectorySettings();

		dir1 = new S3Directory(bucketName, path);
		dir1.create();

		dir2 = new S3Directory(bucketName, path);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testLocks() throws Exception {
		try (Lock lock1 = dir1.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
			lock1.ensureValid();
			try {
				dir2.obtainLock(IndexWriter.WRITE_LOCK_NAME);
				Assert.fail("lock2 should not have valid lock");
			} catch (final IOException e) {
			}
		} finally {
			dir1.close();
			dir1.delete();
			dir2.close();
		}
	}
}
