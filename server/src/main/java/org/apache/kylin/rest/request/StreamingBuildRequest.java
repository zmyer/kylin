/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.request;

import com.google.common.base.Preconditions;
import org.apache.kylin.rest.helix.HelixClusterAdmin;

import static org.apache.kylin.rest.helix.HelixClusterAdmin.RESOURCE_STREAME_CUBE_PREFIX;

public class StreamingBuildRequest {

    private String streaming;
    private long start;
    private long end;
    private String message;
    private boolean successful;

    public StreamingBuildRequest() {
    }

    public StreamingBuildRequest(String streaming, long start, long end) {
        this.streaming = streaming;
        this.start = start;
        this.end = end;
    }

    public String getStreaming() {
        return streaming;
    }

    public void setStreaming(String streaming) {
        this.streaming = streaming;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public void setSuccessful(boolean successful) {
        this.successful = successful;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public String toResourceName() {
        return HelixClusterAdmin.RESOURCE_STREAME_CUBE_PREFIX + streaming + "_" + start + "_" + end;
    }

    public static StreamingBuildRequest fromResourceName(String resourceName) {
        Preconditions.checkArgument(resourceName.startsWith(RESOURCE_STREAME_CUBE_PREFIX));
        long end = Long.parseLong(resourceName.substring(resourceName.lastIndexOf("_") + 1));
        String temp = resourceName.substring(RESOURCE_STREAME_CUBE_PREFIX.length(), resourceName.lastIndexOf("_"));
        long start = Long.parseLong(temp.substring(temp.lastIndexOf("_") + 1));
        String streamingConfig = temp.substring(0, temp.lastIndexOf("_"));

        return new StreamingBuildRequest(streamingConfig, start, end);
    }
}
