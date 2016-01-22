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

package org.apache.kylin.rest.service;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.hadoop.fs.FileUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import java.io.File;

/**
 */
public class TestBaseWithZookeeper extends LocalFileMetadataTestCase {
    protected static final String zkAddress = "localhost:2199";
    static ZkServer server;
    static boolean zkStarted = false;

    @BeforeClass
    public static void setupResource() throws Exception {
        staticCreateTestMetadata();

        if (zkStarted == false) {
            final File tmpDir = File.createTempFile("KylinTest", null);
            FileUtil.fullyDelete(tmpDir);
            tmpDir.mkdirs();
            tmpDir.deleteOnExit();
            server = new ZkServer(tmpDir.getAbsolutePath() + "/dataDir", tmpDir.getAbsolutePath() + "/logDir", new IDefaultNameSpace() {
                @Override
                public void createDefaultNameSpace(ZkClient zkClient) {
                }
            }, 2199, 1000, 2000);

            server.start();
            zkStarted = true;
            System.setProperty("kylin.zookeeper.address", zkAddress);
        }

    }

    @AfterClass
    public static void tearDownResource() {
        if (server == null) {
            server.shutdown();
            zkStarted = false;
            System.setProperty("kylin.zookeeper.address", "");
        }

        staticCleanupTestMetadata();
    }

}
