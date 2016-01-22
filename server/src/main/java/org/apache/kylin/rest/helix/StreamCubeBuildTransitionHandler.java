package org.apache.kylin.rest.helix;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
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
        StreamingBuildRequest streamingBuildRequest = StreamingBuildRequest.fromResourceName(resourceName);

        final String cubeName = StreamingManager.getInstance(kylinConfig).getStreamingConfig(streamingBuildRequest.getStreaming()).getCubeName();
        final CubeInstance cube = CubeManager.getInstance(kylinConfig).getCube(cubeName);
        for (CubeSegment segment : cube.getSegments()) {
            if (segment.getDateRangeStart() <= streamingBuildRequest.getStart() && segment.getDateRangeEnd() >= streamingBuildRequest.getEnd()) {
                logger.info("Segment " + segment.getName() + " already exist, no need rebuild.");
                return;
            }
        }

        KylinConfigBase.getKylinHome();
        String segmentId = streamingBuildRequest.getStart() + "_" + streamingBuildRequest.getEnd();
        String cmd = KylinConfigBase.getKylinHome() + "/bin/kylin.sh streaming start " + streamingBuildRequest.getStreaming() + " " + segmentId + " -oneoff true -start " + streamingBuildRequest.getStart() + " -end " + streamingBuildRequest.getEnd() + " -streaming " + streamingBuildRequest.getStreaming();
        logger.info("Executing: " + cmd);
        try {
            String line;
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((line = input.readLine()) != null) {
                logger.info(line);
            }
            input.close();
        } catch (IOException err) {
            logger.error("Error happens during build streaming  '" + resourceName + "'", err);
            throw new RuntimeException(err);
        }

    }

    @Transition(to = "STANDBY", from = "LEADER")
    public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
        String resourceName = message.getResourceId().stringify();
        StreamingBuildRequest streamingBuildRequest = StreamingBuildRequest.fromResourceName(resourceName);
        KylinConfigBase.getKylinHome();
        String segmentId = streamingBuildRequest.getStart() + "_" + streamingBuildRequest.getEnd();
        String cmd = KylinConfigBase.getKylinHome() + "/bin/kylin.sh streaming stop " + streamingBuildRequest.getStreaming() + " " + segmentId;
        logger.info("Executing: " + cmd);
        try {
            String line;
            Process p = Runtime.getRuntime().exec(cmd);
            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((line = input.readLine()) != null) {
                logger.info(line);
            }
            input.close();
        } catch (IOException err) {
            logger.error("Error happens during build streaming  '" + resourceName + "'", err);
            throw new RuntimeException(err);
        }
    }

    @Transition(to = "STANDBY", from = "OFFLINE")
    public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {

    }

    @Transition(to = "OFFLINE", from = "STANDBY")
    public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {

    }
}