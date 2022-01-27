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

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.erudika.lucene.store.s3.S3Directory;
import com.erudika.lucene.store.s3.S3FileEntrySettings;
import java.nio.ByteBuffer;
import org.apache.lucene.store.BufferedIndexInput;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * An <code>IndexInput</code> implementation, that for every buffer refill will go and fetch the data from the database.
 *
 * @author kimchy
 */
public class FetchOnBufferReadS3IndexInput extends S3BufferedIndexInput {

	private static final Logger logger = LoggerFactory.getLogger(FetchOnBufferReadS3IndexInput.class);

	private String name;

	// lazy intialize the length
	private long totalLength = -1;

	private long position = 0;

	private S3Directory s3Directory;

	public FetchOnBufferReadS3IndexInput() {
		super("FetchOnBufferReadS3IndexInput");
	}

	@Override
	public void configure(final String name, final S3Directory s3Directory, final S3FileEntrySettings settings)
			throws IOException {
		super.configure(name, s3Directory, settings);
		this.s3Directory = s3Directory;
		this.name = name;
	}

	// Overriding refill here since we can execute a single query to get both
	// the length and the buffer data
	// resulted in not the nicest OO design, where the buffer information is
	// protected in the S3BufferedIndexInput
	// class
	// and code duplication between this method and S3BufferedIndexInput.
	// Performance is much better this way!
	@Override
	protected void refill() throws IOException {
		if (logger.isDebugEnabled()) {
			logger.info("refill({})", name);
		}
		ResponseInputStream<GetObjectResponse> res = s3Directory.getS3().
				getObject(b -> b.bucket(s3Directory.getBucket()).key(name));

		synchronized (this) {
			if (totalLength == -1) {
				totalLength = res.response().contentLength();
			}
		}

		final long start = bufferStart + bufferPosition;
		long end = start + bufferSize;
		if (end > length()) {
			end = length();
		}
		bufferLength = (int) (end - start);
		if (bufferLength <= 0) {
			throw new IOException("read past EOF");
		}

		if (buffer == null) {
			buffer = new byte[bufferSize]; // allocate buffer
			// lazily
			seekInternal(bufferStart);
		}
		// START replace read internal
		readInternal(res, buffer, 0, bufferLength);

		bufferStart = start;
		bufferPosition = 0;
	}

	@Override
	protected synchronized void readInternal(final byte[] b, final int offset, final int length) throws IOException {
		if (logger.isDebugEnabled()) {
			logger.info("readInternal({})", name);
		}
		ResponseInputStream<GetObjectResponse> res = s3Directory.getS3().
				getObject(bd -> bd.bucket(s3Directory.getBucket()).key(name));

		readInternal(res, buffer, 0, bufferLength);

		if (totalLength == -1) {
			totalLength = res.response().contentLength();
		}
	}

	private synchronized void readInternal(final ResponseInputStream<GetObjectResponse> res,
			final byte[] b, final int offset, final int length) throws IOException {
		final long curPos = getFilePointer();
		if (curPos != position) {
			position = curPos;
		}
		res.skip(position);
		res.read(b, offset, length);
		position += length;
	}

	@Override
	protected void seekInternal(final long pos) throws IOException {
		position = pos;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public synchronized long length() {
		if (totalLength == -1) {
			try {
				totalLength = s3Directory.fileLength(name);
			} catch (final IOException e) {
				// do nothing here for now, much better for performance
			}
		}
		return totalLength;
	}

	@Override
	public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
		// TODO Auto-generated method stub
		logger.debug("FetchOnBufferReadS3IndexInput.slice()");

		return new SlicedIndexInput(sliceDescription, this, offset, length);
	}

	/**
	 * Implementation of an IndexInput that reads from a portion of a file.
	 */
	private static final class SlicedIndexInput extends BufferedIndexInput {

		IndexInput base;
		long fileOffset;
		long length;

		SlicedIndexInput(final String sliceDescription, final IndexInput base, final long offset, final long length) {
			super(sliceDescription == null ? base.toString() : base.toString() + " [slice=" + sliceDescription + "]",
					BufferedIndexInput.BUFFER_SIZE);
			if (offset < 0 || length < 0 || offset + length > base.length()) {
				throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + base);
			}
			this.base = base.clone();
			fileOffset = offset;
			this.length = length;
		}

		@Override
		public SlicedIndexInput clone() {
			final SlicedIndexInput clone = (SlicedIndexInput) super.clone();
			clone.base = base.clone();
			clone.fileOffset = fileOffset;
			clone.length = length;
			return clone;
		}

		@Override
		protected void readInternal(ByteBuffer bb) throws IOException {
//			final long start = getFilePointer();
//			if (start + len > length) {
//				throw new EOFException("read past EOF: " + this);
//			}
//			System.out.println(name);
//			base.seek(fileOffset + start);
//			base.readBytes(bb.array(), (int) fileOffset, (int) length, false);
			throw new UnsupportedOperationException("THE PROBLEM IS HERE! - IF ANYONE CAN FIX THIS - PRs ARE OPEN.");
		}

		@Override
		protected void seekInternal(final long pos) {
		}

		@Override
		public void close() throws IOException {
			base.close();
		}

		@Override
		public long length() {
			return length;
		}
	}
}
