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

import java.util.HashMap;
import java.util.Map;

import com.erudika.lucene.store.s3.handler.ActualDeleteFileEntryHandler;
import com.erudika.lucene.store.s3.handler.NoOpFileEntryHandler;
import com.erudika.lucene.store.s3.index.FetchOnOpenS3IndexInput;
import com.erudika.lucene.store.s3.index.RAMS3IndexOutput;

/**
 * General directory level settings.
 * <p />
 * The settings also holds {@link S3FileEntrySettings}, that can be registered with the directory settings. Note, that
 * when registering them, they are registered under both the complete name and the 3 charecters name suffix.
 * <p />
 * When creating the settings, it already holds sensible settings, they are: The default {@link S3FileEntrySettings}
 * uses the file entry settings defaults. The "deletable", ""deleteable.new", and "deletable.new" uses the
 * {@link org.apache.lucene.store.s3.handler.NoOpFileEntryHandler}. The "segments" and "segments.new" uses the null {@link org.apache.lucene.store.s3.handler.ActualDeleteFileEntryHandler},
 * {@link org.apache.lucene.store.s3.index.FetchOnOpenS3IndexInput}, and
 * {@link org.apache.lucene.store.s3.index.RAMS3IndexOutput}. The file suffix "fnm" uses the
 * {@link org.apache.lucene.store.s3.index.FetchOnOpenS3IndexInput}, and
 * {@link org.apache.lucene.store.s3.index.RAMS3IndexOutput}. The file suffix "del" and "tmp" uses the
 * {@link org.apache.lucene.store.s3.handler.ActualDeleteFileEntryHandler}.
 *
 * @author kimchy
 */
public class S3DirectorySettings {

	/**
	 * The default file entry settings name that are registered under.
	 */
	public static String DEFAULT_FILE_ENTRY = "__default__";

	/**
	 * A simple constant having the millisecond value of an hour.
	 */
	public static final long HOUR = 60 * 60 * 1000;

	private final HashMap<String, S3FileEntrySettings> fileEntrySettings = new HashMap<String, S3FileEntrySettings>();

	/**
	 * Creates a new instance of the S3 directory settings with it's default values initialized.
	 */
	public S3DirectorySettings() {
		final S3FileEntrySettings defaultSettings = new S3FileEntrySettings();
		fileEntrySettings.put(DEFAULT_FILE_ENTRY, defaultSettings);

		final S3FileEntrySettings deletableSettings = new S3FileEntrySettings();
		deletableSettings.setClassSetting(S3FileEntrySettings.FILE_ENTRY_HANDLER_TYPE, NoOpFileEntryHandler.class);
		fileEntrySettings.put("deletable", deletableSettings);
		fileEntrySettings.put("deleteable.new", deletableSettings);
		// in case lucene fix the spelling mistake
		fileEntrySettings.put("deletable.new", deletableSettings);

		final S3FileEntrySettings segmentsSettings = new S3FileEntrySettings();
		segmentsSettings.setClassSetting(S3FileEntrySettings.FILE_ENTRY_HANDLER_TYPE, ActualDeleteFileEntryHandler.class);
		segmentsSettings.setClassSetting(S3FileEntrySettings.INDEX_INPUT_TYPE_SETTING, FetchOnOpenS3IndexInput.class);
		segmentsSettings.setClassSetting(S3FileEntrySettings.INDEX_OUTPUT_TYPE_SETTING, RAMS3IndexOutput.class);
		fileEntrySettings.put("segments", segmentsSettings);
		fileEntrySettings.put("segments.new", segmentsSettings);

		final S3FileEntrySettings dotDelSettings = new S3FileEntrySettings();
		dotDelSettings.setClassSetting(S3FileEntrySettings.FILE_ENTRY_HANDLER_TYPE,
				ActualDeleteFileEntryHandler.class);
		fileEntrySettings.put("del", dotDelSettings);

		final S3FileEntrySettings tmpSettings = new S3FileEntrySettings();
		tmpSettings.setClassSetting(S3FileEntrySettings.FILE_ENTRY_HANDLER_TYPE, ActualDeleteFileEntryHandler.class);
		fileEntrySettings.put("tmp", dotDelSettings);

		final S3FileEntrySettings fnmSettings = new S3FileEntrySettings();
		fnmSettings.setClassSetting(S3FileEntrySettings.INDEX_INPUT_TYPE_SETTING, FetchOnOpenS3IndexInput.class);
		fnmSettings.setClassSetting(S3FileEntrySettings.INDEX_OUTPUT_TYPE_SETTING, RAMS3IndexOutput.class);
		fileEntrySettings.put("fnm", fnmSettings);
	}

	/**
	 * Registers a {@link S3FileEntrySettings} against the given name. The name can be the full name of the file, or
	 * it's 3 charecters suffix.
	 */
	public void registerFileEntrySettings(final String name, final S3FileEntrySettings fileEntrySettings) {
		this.fileEntrySettings.put(name, fileEntrySettings);
	}

	/**
	 * Returns the file entries map. Please don't change it during runtime.
	 */
	public Map<String, S3FileEntrySettings> getFileEntrySettings() {
		return fileEntrySettings;
	}

	/**
	 * Returns the file entries according to the name. If a direct match is found, it's registered
	 * {@link S3FileEntrySettings} is returned. If one is registered against the last 3 charecters, then it is returned.
	 * If none is found, the default file entry handler is returned.
	 */
	public S3FileEntrySettings getFileEntrySettings(final String name) {
		final S3FileEntrySettings settings = getFileEntrySettingsWithoutDefault(name);
		if (settings != null) {
			return settings;
		}
		return getDefaultFileEntrySettings();
	}

	/**
	 * Same as {@link #getFileEntrySettings(String)}, only returns <code>null</code> if no match is found (instead of
	 * the default file entry handler settings).
	 */
	public S3FileEntrySettings getFileEntrySettingsWithoutDefault(final String name) {
		final S3FileEntrySettings settings = fileEntrySettings.get(name.substring(name.length() - 3));
		if (settings != null) {
			return settings;
		}
		return fileEntrySettings.get(name);
	}

	/**
	 * Returns the default file entry handler settings.
	 */
	public S3FileEntrySettings getDefaultFileEntrySettings() {
		return fileEntrySettings.get(DEFAULT_FILE_ENTRY);
	}
}
