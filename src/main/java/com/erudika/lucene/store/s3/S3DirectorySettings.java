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
import com.erudika.lucene.store.s3.handler.NoOpFileEntryHandler;
import com.erudika.lucene.store.s3.index.RAMS3IndexOutput;
import com.erudika.lucene.store.s3.index.S3IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * General directory level settings.
 * <p />
 * The settings also holds {@link com.erudika.lucene.store.s3.S3FileEntrySettings}, that can be registered with the directory settings. Note, that
 * when registering them, they are registered under both the complete name and the 3 charecters name suffix.
 * <p />
 * When creating the settings, it already holds sensible settings, they are: The default {@link com.erudika.lucene.store.s3.S3FileEntrySettings}
 * uses the file entry settings defaults. The "deletable", ""deleteable.new", and "deletable.new" uses the
 * {@link com.erudika.lucene.store.s3.handler.NoOpFileEntryHandler}. The "segments" and "segments.new" uses the null {@link com.erudika.lucene.store.s3.handler.ActualDeleteFileEntryHandler},
 * {@link com.erudika.lucene.store.s3.index.FetchOnOpenS3IndexInput}, and
 * {@link com.erudika.lucene.store.s3.index.RAMS3IndexOutput}. The file suffix "fnm" uses the
 * {@link com.erudika.lucene.store.s3.index.FetchOnOpenS3IndexInput}, and
 * {@link com.erudika.lucene.store.s3.index.RAMS3IndexOutput}. The file suffix "del" and "tmp" uses the
 * {@link com.erudika.lucene.store.s3.handler.ActualDeleteFileEntryHandler}.
 *
 * @author kimchy
 */
public class S3DirectorySettings {

	/**
	 * The default file entry settings name that are registered under.
	 */
	public static String DEFAULT_FILE_ENTRY = "__default__";
	private static final Logger logger = LoggerFactory.getLogger(S3DirectorySettings.class);

	/**
	 * A simple constant having the millisecond value of an hour.
	 */
	public static final long HOUR = 60 * 60 * 1000;

	private final HashMap<String, com.erudika.lucene.store.s3.S3FileEntrySettings> fileEntrySettings = new HashMap<String, com.erudika.lucene.store.s3.S3FileEntrySettings>();

	/**
	 * Creates a new instance of the S3 directory settings with it's default values initialized.
	 */
	public S3DirectorySettings() {
		final com.erudika.lucene.store.s3.S3FileEntrySettings defaultSettings = new com.erudika.lucene.store.s3.S3FileEntrySettings();
		fileEntrySettings.put(DEFAULT_FILE_ENTRY, defaultSettings);

		final com.erudika.lucene.store.s3.S3FileEntrySettings deletableSettings = new com.erudika.lucene.store.s3.S3FileEntrySettings();
		deletableSettings.setClassSetting(com.erudika.lucene.store.s3.S3FileEntrySettings.FILE_ENTRY_HANDLER_TYPE, NoOpFileEntryHandler.class);
		fileEntrySettings.put("deletable", deletableSettings);
		fileEntrySettings.put("deleteable.new", deletableSettings);
		// in case lucene fix the spelling mistake
		fileEntrySettings.put("deletable.new", deletableSettings);

		final com.erudika.lucene.store.s3.S3FileEntrySettings segmentsSettings = new com.erudika.lucene.store.s3.S3FileEntrySettings();
		segmentsSettings.setClassSetting(com.erudika.lucene.store.s3.S3FileEntrySettings.FILE_ENTRY_HANDLER_TYPE, ActualDeleteFileEntryHandler.class);
		//todo
//		segmentsSettings.setClassSetting(S3FileEntrySettings.INDEX_INPUT_TYPE_SETTING, FetchOnOpenS3IndexInput.class);
		segmentsSettings.setClassSetting(com.erudika.lucene.store.s3.S3FileEntrySettings.INDEX_INPUT_TYPE_SETTING, S3IndexInput.class);
		segmentsSettings.setClassSetting(com.erudika.lucene.store.s3.S3FileEntrySettings.INDEX_OUTPUT_TYPE_SETTING, RAMS3IndexOutput.class);
		fileEntrySettings.put("segments", segmentsSettings);
		fileEntrySettings.put("segments.new", segmentsSettings);
		fileEntrySettings.put("doc", segmentsSettings);

		final com.erudika.lucene.store.s3.S3FileEntrySettings dotDelSettings = new com.erudika.lucene.store.s3.S3FileEntrySettings();
		dotDelSettings.setClassSetting(com.erudika.lucene.store.s3.S3FileEntrySettings.FILE_ENTRY_HANDLER_TYPE,
				ActualDeleteFileEntryHandler.class);
		fileEntrySettings.put("del", dotDelSettings);

		final com.erudika.lucene.store.s3.S3FileEntrySettings tmpSettings = new com.erudika.lucene.store.s3.S3FileEntrySettings();
		tmpSettings.setClassSetting(com.erudika.lucene.store.s3.S3FileEntrySettings.FILE_ENTRY_HANDLER_TYPE, ActualDeleteFileEntryHandler.class);
		fileEntrySettings.put("tmp", dotDelSettings);

		final com.erudika.lucene.store.s3.S3FileEntrySettings fnmSettings = new com.erudika.lucene.store.s3.S3FileEntrySettings();
//		fnmSettings.setClassSetting(S3FileEntrySettings.INDEX_INPUT_TYPE_SETTING, FetchOnOpenS3IndexInput.class);
		fnmSettings.setClassSetting(com.erudika.lucene.store.s3.S3FileEntrySettings.INDEX_INPUT_TYPE_SETTING, S3IndexInput.class);
		fnmSettings.setClassSetting(com.erudika.lucene.store.s3.S3FileEntrySettings.INDEX_OUTPUT_TYPE_SETTING, RAMS3IndexOutput.class);
		fileEntrySettings.put("fnm", fnmSettings);
	}

	/**
	 * Registers a {@link com.erudika.lucene.store.s3.S3FileEntrySettings} against the given name. The name can be the full name of the file, or
	 * it's 3 charecters suffix.
	 */
	public void registerFileEntrySettings(final String name, final com.erudika.lucene.store.s3.S3FileEntrySettings fileEntrySettings) {
		this.fileEntrySettings.put(name, fileEntrySettings);
	}

	/**
	 * Returns the file entries map. Please don't change it during runtime.
	 */
	public Map<String, com.erudika.lucene.store.s3.S3FileEntrySettings> getFileEntrySettings() {
		return fileEntrySettings;
	}

	/**
	 * Returns the file entries according to the name. If a direct match is found, it's registered
	 * {@link com.erudika.lucene.store.s3.S3FileEntrySettings} is returned. If one is registered against the last 3 charecters, then it is returned.
	 * If none is found, the default file entry handler is returned.
	 */
	public com.erudika.lucene.store.s3.S3FileEntrySettings getFileEntrySettings(final String name) {
		final com.erudika.lucene.store.s3.S3FileEntrySettings settings = getFileEntrySettingsWithoutDefault(name);
		if (settings != null) {
			return settings;
		}
		return getDefaultFileEntrySettings();
	}

	/**
	 * Same as {@link #getFileEntrySettings(String)}, only returns <code>null</code> if no match is found (instead of
	 * the default file entry handler settings).
	 */
	public com.erudika.lucene.store.s3.S3FileEntrySettings getFileEntrySettingsWithoutDefault(final String name) {
		final com.erudika.lucene.store.s3.S3FileEntrySettings settings = fileEntrySettings.get(name.substring(name.length() - 3));
		if (settings != null) {
			return settings;
		}
		return fileEntrySettings.get(name);
	}

	/**
	 * Returns the default file entry handler settings.
	 */
	public S3FileEntrySettings getDefaultFileEntrySettings() {
		logger.info("Returning default file entry settings");
		return fileEntrySettings.get(DEFAULT_FILE_ENTRY);
	}
}
