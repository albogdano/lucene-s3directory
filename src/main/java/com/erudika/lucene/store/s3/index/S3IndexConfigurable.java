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

import java.io.IOException;

/**
 * An additional interface that each implementation of <code>IndexInput</code> and <code>IndexOutput</code> must
 * implement. Used to configure newly created <code>IndexInput</code> and <code>IndexOutput</code> S3 based
 * implementation.
 *
 * @author kimchy
 */
public interface S3IndexConfigurable {

	/**
	 * Configures the newly created <code>IndexInput</code> or <code>IndexOutput</code> implementations.
	 *
	 * @param name The name of the file entry
	 * @param s3Directory The S3 directory instance
	 * @param settings The relevant file entry settings
	 * @throws IOException
	 */
	void configure(String name, S3Directory s3Directory, S3FileEntrySettings settings) throws IOException;
}
