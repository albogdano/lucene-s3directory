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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.erudika.lucene.store.s3.handler.FileEntryHandler;
import com.erudika.lucene.store.s3.lock.S3LegalHoldLock;
import com.erudika.lucene.store.s3.support.LuceneFileNames;
import java.util.LinkedList;
import java.util.stream.Collectors;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import com.erudika.lucene.store.s3.lock.S3Lock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.index.IndexWriter;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

/**
 * A S3 based implementation of a Lucene <code>Directory</code> allowing the storage of a Lucene index within S3.
 * The directory works against a single bucket, where the binary data is stored in <code>objects</code>.
 * Each "object" has an entry in the S3, and different {@link org.apache.lucene.store.s3.handler.FileEntryHandler}
 * can be defines for different files (or files groups).
 *
 * @author kimchy
 */
public class S3Directory extends Directory {

	private static final Logger logger = LoggerFactory.getLogger(S3Directory.class);

	private S3DirectorySettings settings;

	private final ConcurrentHashMap<String, Long> fileSizes = new ConcurrentHashMap<>();

	private final HashMap<String, FileEntryHandler> fileEntryHandlers = new HashMap<String, FileEntryHandler>();

	private String bucket;

	private final S3Client s3 = S3Client.builder().
			credentialsProvider(ProfileCredentialsProvider.builder().build()).build();

	/**
	 * Creates a new S3 directory.
	 *
	 * @param bucketName The bucket name
	 * @throws S3StoreException
	 */
	public S3Directory(final String bucketName) throws S3StoreException {
		initialize(bucketName, new S3DirectorySettings());
	}

	/**
	 * Creates a new S3 directory.
	 *
	 * @param bucketName The table name that will be used
	 * @param settings The settings to configure the directory
	 */
	public S3Directory(final String bucketName, final S3DirectorySettings settings) {
		initialize(bucketName, settings);
	}

	private void initialize(final String bucket, S3DirectorySettings settings) {
		this.bucket = bucket.toLowerCase();
		this.settings = settings;
		final Map<String, S3FileEntrySettings> fileEntrySettings = settings.getFileEntrySettings();
		// go over all the file entry settings and configure them
		for (final String name : fileEntrySettings.keySet()) {
			final S3FileEntrySettings feSettings = fileEntrySettings.get(name);
			try {
				final Class<?> fileEntryHandlerClass = feSettings
						.getSettingAsClass(S3FileEntrySettings.FILE_ENTRY_HANDLER_TYPE, null);
				final FileEntryHandler fileEntryHandler = (FileEntryHandler) fileEntryHandlerClass.getConstructor().newInstance();
				fileEntryHandler.configure(this);
				fileEntryHandlers.put(name, fileEntryHandler);
			} catch (final Exception e) {
				throw new IllegalArgumentException("Failed to create FileEntryHandler  ["
						+ feSettings.getSetting(S3FileEntrySettings.FILE_ENTRY_HANDLER_TYPE) + "]");
			}
		}
	}

	/**
	 * *********************************************************************************************
	 * CUSTOM METHODS *********************************************************************************************
	 */
	/**
	 * Returns <code>true</code> if the S3 bucket exists.
	 *
	 * @return <code>true</code> if the S3 bucket exists, <code>false</code> otherwise
	 * @throws java.io.IOException
	 * @throws UnsupportedOperationException If the S3 dialect does not support it
	 */
	public boolean bucketExists() {
		try {
			if (logger.isDebugEnabled()) {
				logger.info("bucketExists({})", bucket);
			}
			s3.headBucket(b -> b.bucket(bucket));
			return true;
		} catch (AwsServiceException | SdkClientException e) {
			return false;
		}
	}

	/**
	 * @param name
	 * @return
	 * @throws java.io.IOException
	 */
	public boolean fileExists(final String name) throws IOException {
		return getFileEntryHandler(name).fileExists(name);
	}

	/**
	 * Deletes the S3 bucket (drops it) from the S3.
	 */
	public void delete() {
		if (bucketExists()) {
			if (logger.isDebugEnabled()) {
				logger.info("delete({})", bucket);
			}
			emptyBucket();
			try {
				s3.deleteBucket(b -> b.bucket(bucket));
			} catch (Exception e) {
				logger.error("Bucket {} not empty - [{}]", bucket, e);
			}
		}
	}

	/**
	 * Creates a new S3 bucket.
	 *
	 * @throws java.io.IOException
	 */
	public void create() {
		if (!bucketExists()) {
			if (logger.isDebugEnabled()) {
				logger.info("create({})", bucket);
			}
			s3.createBucket(b -> b.bucket(bucket).objectLockEnabledForBucket(true));
		}
		try {
			if (logger.isDebugEnabled()) {
				logger.info("write.lock created in {}", bucket);
			}
			// initialize the write.lock file immediately after bucket creation
			s3.putObject(b -> b.bucket(bucket).key(IndexWriter.WRITE_LOCK_NAME), RequestBody.empty());
		} catch (Exception e) { }
	}

	/**
	 * Empties a bucket on S3.
	 */
	public void emptyBucket() {
		deleteObjectVersions(null);
	}

	/**
	 * Deletes all object versions for a given prefix. If prefix is null, all objects are deleted.
	 * @param prefix a key prefix for filtering
	 */
	private void deleteObjectVersions(String prefix) {
		LinkedList<ObjectIdentifier> objects = new LinkedList<>();
		s3.listObjectVersionsPaginator(b -> b.bucket(bucket).prefix(prefix)).forEach((response) -> {
			if (logger.isDebugEnabled()) {
				logger.info("deleteContent({}, {})", bucket, response.versions().size());
			}
			response.versions().forEach((content) -> {
				objects.add(ObjectIdentifier.builder().key(content.key()).versionId(content.versionId()).build());
			});
		});

		List<ObjectIdentifier> keyz = new LinkedList<>();
		for (ObjectIdentifier key : objects) {
			keyz.add(key);
			if (keyz.size() >= 1000) {
				s3.deleteObjects(b -> b.bucket(bucket).delete(bd -> bd.objects(keyz)));
				keyz.clear();
			}
			fileSizes.remove(key.key());
		}
		if (!keyz.isEmpty()) {
			s3.deleteObjects(b -> b.bucket(bucket).delete(bd -> bd.objects(keyz)));
		}
	}

	/**
	 * @param name
	 * @throws java.io.IOException
	 */
	public void forceDeleteFile(final String name) throws IOException {
		if (logger.isDebugEnabled()) {
			logger.info("forceDeleteFile({})", name);
		}
		deleteObjectVersions(name);
	}

	/**
	 * @return @throws IOException
	 */
	protected Lock createLock() throws IOException {
		return new S3LegalHoldLock();
	}

	/**
	 * @param name
	 * @return
	 */
	protected FileEntryHandler getFileEntryHandler(final String name) {
		FileEntryHandler handler = fileEntryHandlers.get(name.substring(name.length() - 3));
		if (handler != null) {
			return handler;
		}
		handler = fileEntryHandlers.get(name);
		if (handler != null) {
			return handler;
		}
		return fileEntryHandlers.get(S3DirectorySettings.DEFAULT_FILE_ENTRY);
	}

	/**
	 * ********************************************************************************************
	 * DIRECTORY METHODS
	 * ********************************************************************************************
	 */
	@Override
	public String[] listAll() {
		if (logger.isDebugEnabled()) {
			logger.info("listAll({})", bucket);
		}
		final LinkedList<String> names = new LinkedList<>();
		try {
			ListObjectsV2Iterable responses = s3.listObjectsV2Paginator(b -> b.bucket(bucket));
			for (ListObjectsV2Response response : responses) {
				names.addAll(response.contents().stream().map((obj) -> obj.key()).collect(Collectors.toList()));
			}
		} catch (Exception e) {
			logger.warn("{}", e.toString());
		}
		return names.toArray(new String[]{});
	}

	@Override
	public void deleteFile(final String name) throws IOException {
		if (LuceneFileNames.isStaticFile(name)) {
			// TODO is necessary??
			logger.warn("S3Directory.deleteFile({}), is static file", name);
			forceDeleteFile(name);
		} else {
			getFileEntryHandler(name).deleteFile(name);
		}
	}

	@Override
	public long fileLength(final String name) throws IOException {
		return getFileEntryHandler(name).fileLength(name);
	}

	@Override
	public IndexOutput createOutput(final String name, final IOContext context) throws IOException {
		if (LuceneFileNames.isStaticFile(name)) {
			// TODO is necessary??
			logger.warn("S3Directory.createOutput({}), is static file", name);
			forceDeleteFile(name);
		}
		return getFileEntryHandler(name).createOutput(name);
	}

	@Override
	public IndexInput openInput(final String name, final IOContext context) throws IOException {
		return getFileEntryHandler(name).openInput(name);
	}

	@Override
	public void sync(final Collection<String> names) throws IOException {
//		logger.warn("S3Directory.sync({})", names);
//		for (final String name : names) {
//			if (!fileExists(name)) {
//				throw new S3StoreException("Failed to sync, file " + name + " not found");
//			}
//		}
	}

	@Override
	public void rename(final String from, final String to) throws IOException {
		getFileEntryHandler(from).renameFile(from, to);
	}

	@Override
	public Lock obtainLock(final String name) throws IOException {
		final Lock lock = createLock();
		((S3Lock) lock).configure(this, name);
		((S3Lock) lock).obtain();
		return lock;
	}

	@Override
	public void close() throws IOException {
		IOException last = null;
		for (final FileEntryHandler fileEntryHandler : fileEntryHandlers.values()) {
			try {
				fileEntryHandler.close();
			} catch (final IOException e) {
				last = e;
			}
		}
		if (last != null) {
			throw last;
		}
	}

	@Override
	public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
		String name = prefix.concat("_temp_").concat(suffix).concat(".tmp");
		if (LuceneFileNames.isStaticFile(name)) {
			// TODO is necessary??
			logger.warn("S3Directory.createOutput({}), is static file", name);
			forceDeleteFile(name);
		}
		return getFileEntryHandler(name).createOutput(name);
	}

	@Override
	public void syncMetaData() throws IOException {
	}

	/**
	 * *********************************************************************************************
	 * SETTER/GETTERS METHODS
	 * *********************************************************************************************
	 */
	public String getBucket() {
		return bucket;
	}

	public S3DirectorySettings getSettings() {
		return settings;
	}

	public S3Client getS3() {
		return s3;
	}

	public ConcurrentHashMap<String, Long> getFileSizes() {
		return fileSizes;
	}
}
