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
import java.io.InputStream;
import com.erudika.lucene.store.s3.S3Directory;
import com.erudika.lucene.store.s3.S3FileEntrySettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;

/**
 * @author kimchy
 */
public abstract class AbstractS3IndexOutput extends S3BufferedIndexOutput {

	private static final Logger logger = LoggerFactory.getLogger(AbstractS3IndexOutput.class);

	protected String name;

	protected S3Directory s3Directory;

	protected AbstractS3IndexOutput(final String resourceDescription) {
		super(resourceDescription);
	}

	@Override
	public void configure(final String name, final S3Directory s3Directory, final S3FileEntrySettings settings)
			throws IOException {
		super.configure(name, s3Directory, settings);
		this.name = name;
		this.s3Directory = s3Directory;
	}

	@Override
	public void close() throws IOException {
		super.close();
		doBeforeClose();
		try {
			if (logger.isDebugEnabled()) {
				logger.info("close({})", name);
			}
			final InputStream is = openInputStream();
			s3Directory.getFileSizes().put(name, length());
			s3Directory.getS3().putObject(b -> b.bucket(s3Directory.getBucket()).key(name),
					RequestBody.fromInputStream(is, length()));
		} catch (Exception e) {
			logger.error(null, e);
		}
		doAfterClose();
	}

	protected abstract InputStream openInputStream() throws IOException;

	protected void doAfterClose() throws IOException {

	}

	protected void doBeforeClose() throws IOException {

	}
}
