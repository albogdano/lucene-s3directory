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
package com.erudika.lucene.store.s3.support;

import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.index.IndexFileNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of utility methods for index file names.
 *
 * @author kimchy
 */
public class LuceneFileNames {

	public static final Logger logger = LoggerFactory.getLogger(LuceneFileNames.class);

	private static final Set<String> STATIC_FILES;

	static {
		STATIC_FILES = new HashSet<String>();
		STATIC_FILES.add(IndexFileNames.SEGMENTS);
		STATIC_FILES.add(IndexFileNames.OLD_SEGMENTS_GEN);
		STATIC_FILES.add(IndexFileNames.PENDING_SEGMENTS);
		STATIC_FILES.add("clearcache");
		STATIC_FILES.add("spellcheck.version");
	}

	/**
	 * Returns if this file name is a static file. A static file is a file that is updated and changed by Lucene.
	 */
	public static boolean isStaticFile(final String name) {
		logger.debug("LuceneFileNames.isStaticFile({})", name);
		return STATIC_FILES.contains(name);
	}

	/**
	 * Returns if the name is a segment file or not.
	 */
	public static boolean isSegmentsFile(final String name) {
		logger.debug("LuceneFileNames.isSegmentsFile({})", name);
		return name.equals(IndexFileNames.SEGMENTS) || name.equals(IndexFileNames.OLD_SEGMENTS_GEN)
				|| name.equals(IndexFileNames.PENDING_SEGMENTS);
	}

}
