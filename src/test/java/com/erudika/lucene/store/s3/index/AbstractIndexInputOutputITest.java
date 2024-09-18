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
package com.erudika.lucene.store.s3.index;

import static com.erudika.lucene.store.s3.S3DirectoryGeneralOperationsITest.bucketName;
import static com.erudika.lucene.store.s3.S3DirectoryGeneralOperationsITest.path;

import com.erudika.lucene.store.s3.AbstractS3DirectoryITest;
import com.erudika.lucene.store.s3.S3Directory;
import com.erudika.lucene.store.s3.S3DirectorySettings;
import com.erudika.lucene.store.s3.S3FileEntrySettings;
import java.io.IOException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author kimchy
 */
public abstract class AbstractIndexInputOutputITest extends AbstractS3DirectoryITest {

	protected S3Directory s3Directory;

	@Before
	public void setUp() throws Exception {
		final S3DirectorySettings settings = new S3DirectorySettings();
		settings.getDefaultFileEntrySettings().setClassSetting(S3FileEntrySettings.INDEX_INPUT_TYPE_SETTING,
				indexInputClass());
		settings.getDefaultFileEntrySettings().setClassSetting(S3FileEntrySettings.INDEX_OUTPUT_TYPE_SETTING,
				indexOutputClass());

		s3Directory = new S3Directory(bucketName, path);
		s3Directory.create();
	}

	@After
	public void tearDown() throws Exception {
		s3Directory.close();
	}

	protected abstract Class<? extends IndexInput> indexInputClass();

	protected abstract Class<? extends IndexOutput> indexOutputClass();

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

	private void innerTestSize(final int bufferSize) throws IOException {
		s3Directory.getSettings().getDefaultFileEntrySettings()
				.setIntSetting(S3BufferedIndexInput.BUFFER_SIZE_SETTING, bufferSize);
		s3Directory.getSettings().getDefaultFileEntrySettings()
				.setIntSetting(S3BufferedIndexOutput.BUFFER_SIZE_SETTING, bufferSize);

		insertData();
		verifyData();
	}

	private void innertTestSizeWithinTransaction(final int bufferSize) throws IOException {
		s3Directory.getSettings().getDefaultFileEntrySettings()
				.setIntSetting(S3BufferedIndexInput.BUFFER_SIZE_SETTING, bufferSize);
		s3Directory.getSettings().getDefaultFileEntrySettings()
				.setIntSetting(S3BufferedIndexOutput.BUFFER_SIZE_SETTING, bufferSize);

		insertData();
		verifyData();
	}

	private void insertData() throws IOException {
		final byte[] test = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
		try (IndexOutput indexOutput = s3Directory.createOutput("value1", new IOContext())) {
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
		Assert.assertTrue(s3Directory.fileExists("value1"));
		Assert.assertEquals(36, s3Directory.fileLength("value1"));

		try (IndexInput indexInput = s3Directory.openInput("value1", new IOContext())) {
			Assert.assertEquals(-1, indexInput.readInt());
			Assert.assertEquals(10, indexInput.readLong());
			Assert.assertEquals(0, indexInput.readInt());
			Assert.assertEquals(0, indexInput.readInt());
			indexInput.readBytes(test, 0, 8);
			Assert.assertEquals((byte) 1, test[0]);
			Assert.assertEquals((byte) 8, test[7]);
			indexInput.readBytes(test, 0, 5);
			Assert.assertEquals((byte) 1, test[0]);
			Assert.assertEquals((byte) 5, test[4]);

			indexInput.seek(28);
			Assert.assertEquals((byte) 1, indexInput.readByte());
			indexInput.seek(30);
			Assert.assertEquals((byte) 3, indexInput.readByte());
		}
	}
}
