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

import com.erudika.lucene.store.s3.S3Directory;
import com.erudika.lucene.store.s3.S3FileEntrySettings;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;

/**
 * An <code>IndexInput</code> implementation that will read all the relevant data from the S3 when created, and will
 * cache it until it is closed.
 * <p/>
 * Used for small file entries in the database like the segments file.
 *
 * @author kimchy
 */
public class FetchOnOpenS3IndexInput extends IndexInput implements S3IndexConfigurable {

	private static final Logger logger = LoggerFactory.getLogger(FetchOnOpenS3IndexInput.class);

	// There is no synchronizaiton since Lucene RAMDirecoty performs no
	// synchronizations.
	// Need to get to the bottom of it.
	public FetchOnOpenS3IndexInput() {
		super("FetchOnOpenS3IndexInput");
	}

	private long length;

	private int position = 0;

	private byte[] data;

	@Override
	public void configure(final String name, final S3Directory s3Directory, final S3FileEntrySettings settings)
			throws IOException {
		if (logger.isDebugEnabled()) {
			logger.info("configure({})", name);
		}
		ResponseInputStream<GetObjectResponse> res = s3Directory.getS3().
				getObject(b -> b.bucket(s3Directory.getBucket()).key(name));

		synchronized (this) {
			length = res.response().contentLength();
		}
		data = new byte[(int) length];
		res.read(data);
		if (data.length != length) {
			throw new IOException("read past EOF");
		}
	}

	@Override
	public byte readByte() throws IOException {
		return data[position++];
	}

	@Override
	public void readBytes(final byte[] b, final int offset, final int len) throws IOException {
		System.arraycopy(data, position, b, offset, len);
		position += len;
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public long getFilePointer() {
		return position;
	}

	@Override
	public void seek(final long pos) throws IOException {
		position = (int) pos;
	}

	@Override
	public long length() {
		return length;
	}

	@Override
	public IndexInput slice(final String sliceDescription, final long offset, final long length) throws IOException {
		// TODO Auto-generated method stub
		logger.debug("FetchOnOpenS3IndexInput.slice()");
		return null;
	}
}
