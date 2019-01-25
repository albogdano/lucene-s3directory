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

import com.erudika.lucene.store.s3.handler.ActualDeleteFileEntryHandler;
import com.erudika.lucene.store.s3.index.FetchOnBufferReadS3IndexInput;
import com.erudika.lucene.store.s3.index.RAMS3IndexOutput;

import junit.framework.TestCase;

/**
 * @author kimchy
 */
public class S3FileEntrySettingsTest extends TestCase {

	public void testDefaultSettings() throws Exception {
		final S3FileEntrySettings settings = new S3FileEntrySettings();

		assertEquals(3, settings.getProperties().size());
		assertEquals(ActualDeleteFileEntryHandler.class,
				settings.getSettingAsClass(S3FileEntrySettings.FILE_ENTRY_HANDLER_TYPE, null));
		assertEquals(FetchOnBufferReadS3IndexInput.class,
				settings.getSettingAsClass(S3FileEntrySettings.INDEX_INPUT_TYPE_SETTING, null));
		assertEquals(RAMS3IndexOutput.class,
				settings.getSettingAsClass(S3FileEntrySettings.INDEX_OUTPUT_TYPE_SETTING, null));
	}

	public void testSetting() {
		final S3FileEntrySettings settings = new S3FileEntrySettings();
		String value1 = settings.getSetting("value1");
		assertNull(value1);

		value1 = settings.getSetting("value1", "default");
		assertEquals("default", value1);

		settings.setSetting("value1", "val");

		value1 = settings.getSetting("value1");
		assertEquals("val", value1);

		value1 = settings.getSetting("value1", "default");
		assertEquals("val", value1);
	}

	public void testSettingFloat() {
		final S3FileEntrySettings settings = new S3FileEntrySettings();
		float value1 = settings.getSettingAsFloat("value1", 0);
		assertEquals(0f, value1, 0.01);

		settings.setFloatSetting("value1", 1.1f);

		value1 = settings.getSettingAsFloat("value1", 0.0f);
		assertEquals(1.1f, value1, 0.01);
	}

	public void testSettingLong() {
		final S3FileEntrySettings settings = new S3FileEntrySettings();
		long value1 = settings.getSettingAsLong("value1", 0);
		assertEquals(0, value1);

		settings.setLongSetting("value1", 1);

		value1 = settings.getSettingAsLong("value1", 0);
		assertEquals(1, value1);
	}

	public void testSettingInt() {
		final S3FileEntrySettings settings = new S3FileEntrySettings();
		int value1 = settings.getSettingAsInt("value1", 0);
		assertEquals(0, value1);

		settings.setIntSetting("value1", 1);

		value1 = settings.getSettingAsInt("value1", 0);
		assertEquals(1, value1);
	}

	public void testSettingBoolean() {
		final S3FileEntrySettings settings = new S3FileEntrySettings();
		boolean value1 = settings.getSettingAsBoolean("value1", false);
		assertFalse(value1);

		settings.setBooleanSetting("value1", true);

		value1 = settings.getSettingAsBoolean("value1", false);
		assertTrue(value1);
	}

	public void testSettingClass() throws Exception {
		final S3FileEntrySettings settings = new S3FileEntrySettings();
		Class<?> value1 = settings.getSettingAsClass("value1", Class.class);
		assertEquals(Class.class, value1);

		settings.setClassSetting("value1", Object.class);

		value1 = settings.getSettingAsClass("value1", Class.class);
		assertEquals(Object.class, value1);
	}
}
