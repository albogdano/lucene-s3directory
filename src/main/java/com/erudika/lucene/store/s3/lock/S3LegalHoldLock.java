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
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHold;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus;
import software.amazon.awssdk.services.s3.model.PutObjectLegalHoldRequest;

import java.io.IOException;

import static software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus.OFF;
import static software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus.ON;

/**
 * <p>
 * A lock based on legal holds on S3 objects.
 *
 * <p>
 * The benefits of using this lock is the ability to release it.
 *
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class S3LegalHoldLock extends Lock implements S3Lock {

	private static final Logger logger = LoggerFactory.getLogger(S3LegalHoldLock.class);

	private S3Directory s3Directory;
	private String name;

	@Override
	public void configure(final S3Directory s3Directory, final String name) throws IOException {
		this.s3Directory = s3Directory;
		this.name = name;
	}

	@Override
	public void obtain() throws IOException {
		try {
			if (logger.isDebugEnabled()) {
				logger.info("obtain({})", name);
			}
			putObjectLegalHold(ON);
		} catch (AwsServiceException | SdkClientException e) {
			throw new LockObtainFailedException("Lock object could not be created: ", e);
		}
	}

	@Override
	public void close() throws IOException {
		try {
			if (logger.isDebugEnabled()) {
				logger.info("close({})", name);
			}
//			res = s3Directory.getS3().getObjectLegalHold(b -> b.bucket(s3Directory.getBucket()).key(name));

			putObjectLegalHold(OFF);
//			LOCKS.remove(name);
		} catch (AwsServiceException | SdkClientException e) {
			throw new AlreadyClosedException("Lock was already released: ", e);
		}

//		if (res != null && res.legalHold().status().equals(OFF)) {
//			throw new AlreadyClosedException("Lock was already released: " + this);
//		}
	}

	@Override
	public void ensureValid() throws IOException {
		try {
			if (logger.isDebugEnabled()) {
				logger.info("ensureValid({})", name);
			}
			if (!isLegalHoldOn()) {
				// TODO should throw AlreadyClosedException??
				throw new AlreadyClosedException("Lock instance already released: " + this);
			}
		} catch (AwsServiceException | SdkClientException e) {
			throw new AlreadyClosedException("Lock object not found: " + this);
		}
	}

	private boolean isLegalHoldOn() {
		return s3Directory.getS3().getObjectLegalHold(b ->
				b.bucket(s3Directory.getBucket()).key(name)).legalHold().status().equals(ON);
	}

	private void putObjectLegalHold(ObjectLockLegalHoldStatus status) {

		s3Directory.getS3().putObjectLegalHold(PutObjectLegalHoldRequest
				.builder()
						.bucket(s3Directory.getBucket())
						.key(name)
						.legalHold(ObjectLockLegalHold.builder().status(status).build())
				.build());
	}

	@Override
	public String toString() {
		return "S3LegalHoldLock[" + s3Directory.getBucket() + "/" + name + "]";
	}
}
