/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.data.manager.offline;

import java.io.File;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotFSOfflineSegmentUploader implements OfflineSegmentUploader {

    private static final Logger LOGGER = LoggerFactory.getLogger(PinotFSOfflineSegmentUploader.class);
    public static final int DEFAULT_SEGMENT_UPLOAD_TIMEOUT_MILLIS = 10 * 1000;

    private final String _segmentStoreUriStr;
    private final ExecutorService _executorService = Executors.newCachedThreadPool();
    private final int _timeoutInMs;

    public PinotFSOfflineSegmentUploader(String segmentStoreDirUri, int timeoutMillis) {
        _segmentStoreUriStr = segmentStoreDirUri;
        _timeoutInMs = timeoutMillis;
    }

    @Override
    public boolean uploadSegment(File segmentFile, URI uploadURI) {
        if (_segmentStoreUriStr == null || _segmentStoreUriStr.isEmpty()) {
            LOGGER.error("Missing segment store uri. Failed to upload segment file {} to {}.", segmentFile.getName(),
                    uploadURI.getPath());
            return false;
        }
        Callable<Boolean> uploadTask = () -> {
            try {
                PinotFS pinotFS = PinotFSFactory.create(new URI(_segmentStoreUriStr).getScheme());
                // Check and delete any existing segment file.
                if (pinotFS.exists(uploadURI)) {
                    pinotFS.delete(uploadURI, true);
                }
                pinotFS.copyFromLocalFile(segmentFile, uploadURI);
            } catch (Exception e) {
                LOGGER.warn("Failed copy segment tar file {} to segment store {}: {}",
                        segmentFile.getName(), uploadURI, e);
                return false;
            }
            return false;
        };
        Future<Boolean> future = _executorService.submit(uploadTask);
        try {
            Boolean uploadStatus = future.get(_timeoutInMs, TimeUnit.MILLISECONDS);
            if (uploadStatus) {
                LOGGER.info("Successfully upload segment {} to {}.", segmentFile.getName(), uploadURI.getPath());
                return true;
            }
            LOGGER.warn("Failed to upload segment {} to {}.", segmentFile.getName(), uploadURI.getPath());
            return false;
        } catch (InterruptedException e) {
            LOGGER.info("Interrupted while waiting for segment upload of {} to {}.", segmentFile.getName(), uploadURI);
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            LOGGER.warn("Failed to upload segment {} to {} ", segmentFile.getName(), uploadURI.getPath(), e);
            return false;
        }
    }
}
