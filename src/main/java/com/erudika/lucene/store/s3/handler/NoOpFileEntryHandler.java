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
package com.erudika.lucene.store.s3.handler;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.erudika.lucene.store.s3.S3Directory;

/**
 * A No Operation file entry handler. Performs no actual dirty operations, and returns empty data for read operations.
 *
 * @author kimchy
 */
public class NoOpFileEntryHandler implements FileEntryHandler {

	private static final Logger logger = LoggerFactory.getLogger(NoOpFileEntryHandler.class);

	private static class NoOpIndexInput extends IndexInput {

		protected NoOpIndexInput() {
			super("NoOpIndexInput");
		}

		@Override
		public byte readByte() throws IOException {
			return 0;
		}

		@Override
		public void readBytes(final byte[] b, final int offset, final int len) throws IOException {

		}

		@Override
		public void close() throws IOException {

		}

		@Override
		public long getFilePointer() {
			return 0;
		}

		@Override
		public void seek(final long pos) throws IOException {
		}

		@Override
		public long length() {
			return 0;
		}

		@Override
		public IndexInput slice(final String sliceDescription, final long offset, final long length)
				throws IOException {
			// TODO Auto-generated method stub
			logger.debug("NoOpFileEntryHandler.NoOpIndexInput.slice()");
			return null;
		}
	}

	@SuppressWarnings("unused")
	private static class NoOpIndexOutput extends IndexOutput {

		protected NoOpIndexOutput() {
			super("NoOpIndexOutput", "NoOpIndexOutput");
		}

		@Override
		public void writeByte(final byte b) throws IOException {

		}

		@Override
		public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {

		}

		public void flush() throws IOException {
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public long getFilePointer() {
			return 0;
		}

		public void seek(final long pos) throws IOException {
		}

		public long length() throws IOException {
			return 0;
		}

		@Override
		public long getChecksum() throws IOException {
			// TODO Auto-generated method stub
			logger.debug("NoOpFileEntryHandler.NoOpIndexOutput.slice()");
			return 0;
		}
	}

	private static IndexInput indexInput = new NoOpIndexInput();

	private static IndexOutput indexOutput = new NoOpIndexOutput();

	@Override
	public void configure(final S3Directory s3Directory) {
	}

	@Override
	public boolean fileExists(final String name) throws IOException {
		return false;
	}

	@Override
	public long fileModified(final String name) throws IOException {
		return 0;
	}

	@Override
	public void touchFile(final String name) throws IOException {
	}

	@Override
	public void deleteFile(final String name) throws IOException {
	}

	@Override
	public void renameFile(final String from, final String to) throws IOException {
	}

	@Override
	public long fileLength(final String name) throws IOException {
		return 0;
	}

	@Override
	public IndexInput openInput(final String name) throws IOException {
		return indexInput;
	}

	@Override
	public IndexOutput createOutput(final String name) throws IOException {
		return indexOutput;
	}

	@Override
	public void close() throws IOException {
		// do notihng
	}
}
