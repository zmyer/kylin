/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.engine.streaming;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class StreamingConfig extends RootPersistentEntity {

    public static Serializer<StreamingConfig> SERIALIZER = new JsonSerializer<StreamingConfig>(StreamingConfig.class);

    public static final String STREAMING_TYPE_KAFKA = "kafka";

    @JsonProperty("name")
    private String name;

    @JsonProperty("type")
    private String type = STREAMING_TYPE_KAFKA;

    @JsonProperty("max_gap")
    private long maxGap = 30 * 60 * 1000l; // 30 minutes

    @JsonProperty("max_gap_number")
    private int maxGapNumber = 10; // 10

    @JsonProperty("partitions")
    private Map<String, List<String>> partitions; // realization partition info, key is realization name
    
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getResourcePath() {
        return concatResourcePath(name);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static String concatResourcePath(String name) {
        return ResourceStore.STREAMING_RESOURCE_ROOT + "/" + name + ".json";
    }

    public long getMaxGap() {
        return maxGap;
    }

    public void setMaxGap(long maxGap) {
        this.maxGap = maxGap;
    }

    public int getMaxGapNumber() {
        return maxGapNumber;
    }

    public void setMaxGapNumber(int maxGapNumber) {
        this.maxGapNumber = maxGapNumber;
    }

    public Map<String, List<String>> getPartitions() {
        return partitions;
    }

    public void setPartitions(Map<String, List<String>> partitions) {
        this.partitions = partitions;
    }

    @Override
    public StreamingConfig clone() {
        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            SERIALIZER.serialize(this, new DataOutputStream(baos));
            return SERIALIZER.deserialize(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        } catch (IOException e) {
            throw new RuntimeException(e);//in mem, should not happen
        }
    }

}
