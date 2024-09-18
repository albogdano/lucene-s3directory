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
package com.erudika.lucene.store.s3.lock;

import com.erudika.lucene.store.s3.S3Directory;

import java.io.IOException;

/**
 * An extension insterface for Lucene <code>Lock</code> class.
 *
 * @author kimchy
 */
public interface S3Lock {

	/**
	 * Configures the lock. Called just after the lock is instantiated.
	 *
	 * @param s3Directory The directory using the lock
	 * @param name The name of the lock
	 * @throws IOException
	 */
	void configure(S3Directory s3Directory, String name) throws IOException;

	/**
	 * @throws IOException
	 */
	void obtain() throws IOException;
}
