package org.apache.kylin.rest.helix;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.streaming.StreamingConfig;
import org.apache.kylin.engine.streaming.StreamingManager;
import org.apache.kylin.rest.request.StreamingBuildRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class StreamCubeBuildTransitionHandler extends TransitionHandler {

    private static final Logger logger = LoggerFactory.getLogger(StreamCubeBuildTransitionHandler.class);

    private static ConcurrentMap<KylinConfig, StreamCubeBuildTransitionHandler> instanceMaps = Maps.newConcurrentMap();
    private final KylinConfig kylinConfig;

    private StreamCubeBuildTransitionHandler(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    public static StreamCubeBuildTransitionHandler getInstance(KylinConfig kylinConfig) {
        Preconditions.checkNotNull(kylinConfig);
        instanceMaps.putIfAbsent(kylinConfig, new StreamCubeBuildTransitionHandler(kylinConfig));
        return instanceMaps.get(kylinConfig);
    }

    @Transition(to = "LEADER", from = "STANDBY")
    public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
        String resourceName = message.getResourceId().stringify();
        final StreamingBuildRequest streamingBuildRequest = getStreamingBuildRequest(resourceName, message.getPartitionName());
        if (streamingBuildRequest != null && isSuccessfullyBuilt(streamingBuildRequest) == false) {
            KylinConfigBase.getKylinHome();
            String segmentId = streamingBuildRequest.toPartitionName();
            String cmd = KylinConfigBase.getKylinHome() + "/bin/kylin.sh streaming start " + streamingBuildRequest.getStreaming() + " " + segmentId + " -oneoff true -start " + streamingBuildRequest.getStart() + " -end " + streamingBuildRequest.getEnd() + " -streaming " + streamingBuildRequest.getStreaming();
            runCMD(cmd);
        }
    }

    @Transition(to = "STANDBY", from = "LEADER")
    public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
        String resourceName = message.getResourceId().stringify();
        logger.info("Partition " + message.getPartitionId() + " becomes as Standby");
        /*
        final StreamingBuildRequest streamingBuildRequest = getStreamingBuildRequest(resourceName, message.getPartitionName());
        if (isSuccessfullyBuilt(streamingBuildRequest) == false) {
            KylinConfigBase.getKylinHome();
            String segmentId = streamingBuildRequest.toPartitionName();
            String cmd = KylinConfigBase.getKylinHome() + "/bin/kylin.sh streaming stop " + streamingBuildRequest.getStreaming() + " " + segmentId;
            runCMD(cmd);
        }
        */
    }

    private boolean isSuccessfullyBuilt(StreamingBuildRequest streamingBuildRequest) {
        final StreamingConfig streamingConfig = StreamingManager.getInstance(kylinConfig).getStreamingConfig(streamingBuildRequest.getStreaming());
        final String cubeName = streamingConfig.getCubeName();
        final CubeInstance cube = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        for (CubeSegment segment : cube.getSegments()) {
            if (segment.getDateRangeStart() <= streamingBuildRequest.getStart() && segment.getDateRangeEnd() >= streamingBuildRequest.getEnd()) {
                logger.info("Segment " + segment.getName() + " already exist.");
                return true;
            }
        }

        return false;
    }

    private StreamingBuildRequest getStreamingBuildRequest(String resourceName, String partitionName) {
        String streamConfigName = resourceName.substring(HelixClusterAdmin.RESOURCE_STREAME_CUBE_PREFIX.length());
        int partitionId = Integer.parseInt(partitionName.substring(partitionName.lastIndexOf("_") + 1));

        StreamingConfig streamingConfig = StreamingManager.getInstance(kylinConfig).getStreamingConfig(streamConfigName);

        int retry = 0;
        while ((streamingConfig.getPartitions() == null || streamingConfig.getPartitions().isEmpty() || partitionId > (streamingConfig.getPartitions().size() - 1) && retry < 10)) {
            logger.error("No segment information in StreamingConfig '" + streamConfigName + "' for partition " + partitionId);
            logger.error("Wait for 0.5 second...");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                logger.error("", e);
            }
            streamingConfig = StreamingManager.getInstance(kylinConfig).getStreamingConfig(streamConfigName);
            retry++;
        }

        if (retry >= 10) {
            logger.error("No segment information in StreamingConfig '" + streamConfigName + "' for partition " + partitionId);
            logger.warn("Abor building...");
            return null;
        }

        String startEnd = streamingConfig.getPartitions().get(partitionId);
        long start = Long.parseLong(startEnd.substring(0, startEnd.indexOf("_")));
        long end = Long.parseLong(startEnd.substring(startEnd.indexOf("_") + 1));
        StreamingBuildRequest request = new StreamingBuildRequest();
        request.setStreaming(streamConfigName);
        request.setStart(start);
        request.setEnd(end);
        return request;

    }

    private void runCMD(String cmd) {
        logger.info("Executing: " + cmd);
        BufferedReader input = null;
        Process p = null;
        try {
            String line;
            p = Runtime.getRuntime().exec(cmd);
            input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((line = input.readLine()) != null) {
                logger.info(line);
            }

            logger.info("Successfully start: " + cmd);
        } catch (IOException err) {
            logger.error("Error happens when running '" + cmd + "'", err);
            throw new RuntimeException(err);
        } finally {
            IOUtils.closeQuietly(input);
        }

    }

    @Transition(to = "STANDBY", from = "OFFLINE")
    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {

    }

    @Transition(to = "OFFLINE", from = "STANDBY")
    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {

    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context)
            throws Exception {
        logger.info("Default OFFLINE->DROPPED transition invoked.");
    }

    @Transition(to = "OFFLINE", from = "DROPPED")
    public void onBecomeOfflineFromDropped(Message message, NotificationContext context)
            throws Exception {
        logger.info("Default DROPPED->OFFLINE transition invoked.");
    }

}