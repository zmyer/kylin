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
package org.apache.kylin.rest.helix;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;

import static org.apache.kylin.rest.helix.HelixClusterAdmin.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
*/
public class HelixClusterAdminTest extends LocalFileMetadataTestCase {

    String zkAddress = "localhost:2199";
    ZkServer server;

    HelixClusterAdmin clusterAdmin1;
    HelixClusterAdmin clusterAdmin2;
    KylinConfig kylinConfig;

    private static final String CLUSTER_NAME = "test_cluster";

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        // start zookeeper on localhost
        final File tmpDir = File.createTempFile("HelixClusterAdminTest", null); 
        FileUtil.fullyDelete(tmpDir);
        tmpDir.mkdirs();
        server = new ZkServer(tmpDir.getAbsolutePath() + "/dataDir", tmpDir.getAbsolutePath() + "/logDir", new IDefaultNameSpace() {
            @Override
            public void createDefaultNameSpace(ZkClient zkClient) {
            }
        }, 2199);
        server.start();

        kylinConfig = this.getTestConfig();
        kylinConfig.setRestAddress("localhost:7070");
        kylinConfig.setZookeeperAddress(zkAddress);
        kylinConfig.setClusterName(CLUSTER_NAME);
        
        final ZKHelixAdmin zkHelixAdmin = new ZKHelixAdmin(zkAddress);
        zkHelixAdmin.dropCluster(kylinConfig.getClusterName());

    }

    @Test
    public void test() throws Exception {
        
        // 1. start one instance
        clusterAdmin1 = getInstance(kylinConfig);
        clusterAdmin1.start();

        Thread.sleep(1000);
        assertTrue(clusterAdmin1.isLeaderRole(RESOURCE_NAME_JOB_ENGINE));
        assertEquals(1, kylinConfig.getRestServers().length);
        assertEquals("localhost:7070", kylinConfig.getRestServers()[0]);
        
        // 2. start second instance
        InputStream is = IOUtils.toInputStream(kylinConfig.getConfigAsString());
        KylinConfig kylinConfig2 = KylinConfig.getKylinConfigFromInputStream(is);
        kylinConfig2.setRestAddress("localhost:7072");
        is.close();


        clusterAdmin2 = getInstance(kylinConfig2);
        clusterAdmin2.start();

        Thread.sleep(1000);
        assertTrue(clusterAdmin1.isLeaderRole(RESOURCE_NAME_JOB_ENGINE));
        assertFalse(clusterAdmin2.isLeaderRole(RESOURCE_NAME_JOB_ENGINE));
        assertEquals(2, kylinConfig.getRestServers().length);
        assertEquals("localhost:7070", kylinConfig.getRestServers()[0]);
        assertEquals("localhost:7072", kylinConfig.getRestServers()[1]);
        
        // 3. shutdown the first instance
        clusterAdmin1.stop();
        clusterAdmin1 = null;
        Thread.sleep(1000);
        assertTrue(clusterAdmin2.isLeaderRole(RESOURCE_NAME_JOB_ENGINE));
        assertEquals(1, kylinConfig.getRestServers().length);
        assertEquals("localhost:7072", kylinConfig.getRestServers()[0]);
        
        // 4. recover first instance
        clusterAdmin1 = getInstance(kylinConfig);
        clusterAdmin1.start();

        Thread.sleep(1000);
        assertTrue(clusterAdmin1.isLeaderRole(RESOURCE_NAME_JOB_ENGINE));
        assertFalse(clusterAdmin2.isLeaderRole(RESOURCE_NAME_JOB_ENGINE));
        assertEquals(2, kylinConfig.getRestServers().length);
        assertEquals("localhost:7070", kylinConfig.getRestServers()[0]);
        assertEquals("localhost:7072", kylinConfig.getRestServers()[1]);
    }

    @After
    public void tearDown() {
        if (clusterAdmin1 != null) {
            clusterAdmin1.stop();
        }

        if (clusterAdmin2 != null) {
            clusterAdmin2.stop();
        }
        
        server.shutdown();
        cleanupTestMetadata();
    }

}
