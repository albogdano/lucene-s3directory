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

import com.erudika.lucene.store.s3.S3Directory;
import com.erudika.lucene.store.s3.S3FileEntrySettings;

/**
 * A simple base class that performs index input memory based buffering. The buffer size can be configured under the
 * {@link #BUFFER_SIZE_SETTING} name.
 *
 * @author kimchy
 */
public abstract class S3BufferedIndexInput extends ConfigurableBufferedIndexInput implements S3IndexConfigurable {

	/**
	 * The buffer size setting name. See {@link S3FileEntrySettings#setIntSetting(String, int)}. Should be set in bytes.
	 */
	public static final String BUFFER_SIZE_SETTING = "indexInput.bufferSize";

	protected S3BufferedIndexInput(final String resourceDescription) {
		super(resourceDescription, BUFFER_SIZE);
	}

	@Override
	public void configure(final String name, final S3Directory s3Directory, final S3FileEntrySettings settings)
			throws IOException {
		setBufferSize(settings.getSettingAsInt(BUFFER_SIZE_SETTING, BUFFER_SIZE));
	}
}
