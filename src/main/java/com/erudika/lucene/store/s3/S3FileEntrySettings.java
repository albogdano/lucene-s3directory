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
import java.util.Properties;

/**
 * A file entry level settings. An abstract view of any type of setting that cab be used by the actual file entry
 * handler that uses it.
 * <p/>
 * Holds the {@link #FILE_ENTRY_HANDLER_TYPE} that defines the type of the
 * {@link com.erudika.lucene.store.s3.handler.FileEntryHandler} that will be created and initialized with the settings.
 * <p/>
 * Default values for a new instanciated instnce are:
 * {@link com.erudika.lucene.store.s3.handler.MarkDeleteFileEntryHandler} for the {@link #FILE_ENTRY_HANDLER_TYPE}
 * setting, {@link FetchOnBufferReadS3IndexInput} for the
 * {@link #INDEX_INPUT_TYPE_SETTING} setting, and {@link org.apache.lucene.store.s3.index.RAMAndFileS3IndexOutput} for
 * the {@link #INDEX_OUTPUT_TYPE_SETTING} setting.
 *
 * @author kimchy
 */
public class S3FileEntrySettings {

	/**
	 * The class name of the {@link org.apache.lucene.store.IndexInput}. Only applies to
	 * {@link org.apache.lucene.store.s3.handler.FileEntryHandler}s that use it.
	 */
	public static final String INDEX_INPUT_TYPE_SETTING = "indexInput.type";

	/**
	 * The class name of the {@link org.apache.lucene.store.IndexOutput}. Only applies to
	 * {@link org.apache.lucene.store.s3.handler.FileEntryHandler}s that use it.
	 */
	public static final String INDEX_OUTPUT_TYPE_SETTING = "indexOutput.type";

	/**
	 * The class name of the {@link org.apache.lucene.store.s3.handler.FileEntryHandler}.
	 */
	public static final String FILE_ENTRY_HANDLER_TYPE = "type";

	private final Properties settings = new Properties();

	/**
	 * Creates a new file entry settings, and intialize it to default values.
	 */
	public S3FileEntrySettings() {
		setClassSetting(S3FileEntrySettings.FILE_ENTRY_HANDLER_TYPE, ActualDeleteFileEntryHandler.class);
		setClassSetting(S3FileEntrySettings.INDEX_INPUT_TYPE_SETTING, FetchOnBufferReadS3IndexInput.class);
		setClassSetting(S3FileEntrySettings.INDEX_OUTPUT_TYPE_SETTING, RAMS3IndexOutput.class);
	}

	/**
	 * Returns the inner java properties.
	 */
	public Properties getProperties() {
		return settings;
	}

	/**
	 * Returns the value match for the given setting. <code>null</code> if no setting is found.
	 *
	 * @param setting The setting name
	 * @return The value of the setting, or <code>null</code> if none is found
	 */
	public String getSetting(final String setting) {
		return settings.getProperty(setting);
	}

	/**
	 * Returns the value that matches the given setting. If none is found, the default value is used.
	 *
	 * @param setting The setting name
	 * @param defaultValue The default value to be used if no setting is found
	 * @return The value of the setting, or defaultValue if none is found.
	 */
	public String getSetting(final String setting, final String defaultValue) {
		return settings.getProperty(setting, defaultValue);
	}

	/**
	 * Returns the float value that matches the given setting. If none if found, the default value is used.
	 *
	 * @param setting The setting name
	 * @param defaultValue The default value to be used if no setting is found
	 * @return The value of the setting, or defaultValue if none is found.
	 */
	public float getSettingAsFloat(final String setting, final float defaultValue) {
		final String sValue = getSetting(setting);
		if (sValue == null) {
			return defaultValue;
		}
		return Float.parseFloat(sValue);
	}

	/**
	 * Returns the int value that matches the given setting. If none if found, the default value is used.
	 *
	 * @param setting The setting name
	 * @param defaultValue The default value to be used if no setting is found
	 * @return The value of the setting, or defaultValue if none is found.
	 */
	public int getSettingAsInt(final String setting, final int defaultValue) {
		final String sValue = getSetting(setting);
		if (sValue == null) {
			return defaultValue;
		}
		return Integer.parseInt(sValue);
	}

	/**
	 * Returns the long value that matches the given setting. If none if found, the default value is used.
	 *
	 * @param setting The setting name
	 * @param defaultValue The default value to be used if no setting is found
	 * @return The value of the setting, or defaultValue if none is found.
	 */
	public long getSettingAsLong(final String setting, final long defaultValue) {
		final String sValue = getSetting(setting);
		if (sValue == null) {
			return defaultValue;
		}
		return Long.parseLong(sValue);
	}

	/**
	 * Returns the class value that matches the given setting. If none if found, the default value is used.
	 *
	 * @param setting The setting name
	 * @param defaultValue The default value to be used if no setting is found
	 * @return The value of the setting, or defaultValue if none is found.
	 */
	public Class<?> getSettingAsClass(final String setting, final Class<?> defaultValue) throws ClassNotFoundException {
		return getSettingAsClass(setting, defaultValue, Thread.currentThread().getContextClassLoader());
	}

	/**
	 * Returns the class value that matches the given setting. If none if found, the default value is used.
	 *
	 * @param setting The setting name
	 * @param defaultValue The default value to be used if no setting is found
	 * @param classLoader The class loader to be used to load the class
	 * @return The value of the setting, or defaultValue if none is found.
	 * @throws ClassNotFoundException
	 */
	public Class<?> getSettingAsClass(final String setting, final Class<?> defaultValue, final ClassLoader classLoader)
			throws ClassNotFoundException {
		final String sValue = getSetting(setting);
		if (sValue == null) {
			return defaultValue;
		}
		return Class.forName(sValue, true, classLoader);
	}

	/**
	 * Returns the boolean value that matches the given setting. If none if found, the default value is used.
	 *
	 * @param setting The setting name
	 * @param defaultValue The default value to be used if no setting is found
	 * @return The value of the setting, or defaultValue if none is found.
	 */
	public boolean getSettingAsBoolean(final String setting, final boolean defaultValue) {
		final String sValue = getSetting(setting);
		if (sValue == null) {
			return defaultValue;
		}
		return Boolean.valueOf(sValue).booleanValue();
	}

	public S3FileEntrySettings setSetting(final String setting, final String value) {
		settings.setProperty(setting, value);
		return this;
	}

	public S3FileEntrySettings setBooleanSetting(final String setting, final boolean value) {
		setSetting(setting, String.valueOf(value));
		return this;
	}

	public S3FileEntrySettings setFloatSetting(final String setting, final float value) {
		setSetting(setting, String.valueOf(value));
		return this;
	}

	public S3FileEntrySettings setIntSetting(final String setting, final int value) {
		setSetting(setting, String.valueOf(value));
		return this;
	}

	public S3FileEntrySettings setLongSetting(final String setting, final long value) {
		setSetting(setting, String.valueOf(value));
		return this;
	}

	public S3FileEntrySettings setClassSetting(final String setting, final Class<?> clazz) {
		setSetting(setting, clazz.getName());
		return this;
	}
}
