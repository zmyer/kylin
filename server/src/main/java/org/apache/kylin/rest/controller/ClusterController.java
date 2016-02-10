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

package org.apache.kylin.rest.controller;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.streaming.StreamingManager;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.JobLock;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.helix.HelixClusterAdmin;
import org.apache.kylin.rest.request.StreamingBuildRequest;
import org.apache.kylin.storage.hbase.util.ZookeeperJobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.Collection;

/**
 * 
 */
@Controller
@RequestMapping(value = "cluster")
public class ClusterController extends BasicController implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(ClusterController.class);

    @Autowired
    private JobLock jobLock;
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {

        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (kylinConfig.isClusterEnabled()) {
            final HelixClusterAdmin clusterAdmin = HelixClusterAdmin.getInstance(kylinConfig);
            clusterAdmin.start();

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    clusterAdmin.stop();
                }
            }));
        } else {
            String serverMode = kylinConfig.getServerMode();
            if (Constant.SERVER_MODE_JOB.equals(serverMode.toLowerCase()) || Constant.SERVER_MODE_ALL.equals(serverMode.toLowerCase())) {
                logger.info("Initializing Job Engine ....");
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            DefaultScheduler scheduler = DefaultScheduler.createInstance();
                            scheduler.init(new JobEngineConfig(kylinConfig), jobLock);
                            if (!scheduler.hasStarted()) {
                                logger.error("scheduler has not been started");
                                System.exit(1);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }).start();
            }
        }

    }

}
