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

import junit.framework.TestCase;

/**
 * @author kimchy
 */
public class S3DirectorySettingsTest extends TestCase {

	public void testFileEntrySettings() {
		S3DirectorySettings settings = new S3DirectorySettings();
		S3FileEntrySettings feSettings = new S3FileEntrySettings();
		settings.registerFileEntrySettings("tst", feSettings);
		assertEquals(feSettings, settings.getFileEntrySettings("tst"));
		assertEquals(feSettings, settings.getFileEntrySettings("1.tst"));
		assertEquals(settings.getDefaultFileEntrySettings(), settings.getFileEntrySettings("test"));
		assertEquals(settings.getDefaultFileEntrySettings(), settings.getFileEntrySettings("1.test"));
	}
}
