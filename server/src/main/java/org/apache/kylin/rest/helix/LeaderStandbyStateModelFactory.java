package org.apache.kylin.rest.helix;

import com.google.common.base.Preconditions;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.streaming.OneOffStreamingBuilder;
import org.apache.kylin.engine.streaming.cli.StreamingCLI;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kylin.rest.helix.HelixClusterAdmin.RESOURCE_STREAME_CUBE_PREFIX;

/**
 */
public class LeaderStandbyStateModelFactory extends StateTransitionHandlerFactory<TransitionHandler> {
    private static final Logger logger = LoggerFactory.getLogger(LeaderStandbyStateModelFactory.class);
    
    @Override
    public TransitionHandler createStateTransitionHandler(PartitionId partitionId) {
        if (partitionId.getResourceId().equals(ResourceId.from(HelixClusterAdmin.RESOURCE_NAME_JOB_ENGINE))) {
            return JobEngineStateModel.INSTANCE;
        }

        if (partitionId.getResourceId().stringify().startsWith(RESOURCE_STREAME_CUBE_PREFIX)) {
            return StreamCubeStateModel.INSTANCE;
        }

        return null;
    }

    public static class JobEngineStateModel extends TransitionHandler {
        
        public static JobEngineStateModel INSTANCE = new JobEngineStateModel();

        @Transition(to = "LEADER", from = "STANDBY")
        public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
            logger.info("JobEngineStateModel.onBecomeLeaderFromStandby()");
            try {
                KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
                DefaultScheduler scheduler = DefaultScheduler.createInstance();
                scheduler.init(new JobEngineConfig(kylinConfig), new MockJobLock());
                while (!scheduler.hasStarted()) {
                    logger.error("scheduler has not been started");
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                logger.error("error start DefaultScheduler", e);
                throw new RuntimeException(e);
            }
        }

        @Transition(to = "STANDBY", from = "LEADER")
        public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
            logger.info("JobEngineStateModel.onBecomeStandbyFromLeader()");
            DefaultScheduler.destroyInstance();
            
        }

        @Transition(to = "STANDBY", from = "OFFLINE")
        public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
            logger.info("JobEngineStateModel.onBecomeStandbyFromOffline()");

        }


        @Transition(to = "OFFLINE", from = "STANDBY")
        public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
            logger.info("JobEngineStateModel.onBecomeOfflineFromStandby()");

        }
    }

    public static class StreamCubeStateModel extends TransitionHandler {
        
        public static StreamCubeStateModel INSTANCE = new StreamCubeStateModel();

        @Transition(to = "LEADER", from = "STANDBY")
        public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
            String resourceName = message.getResourceId().stringify();
            Preconditions.checkArgument(resourceName.startsWith(RESOURCE_STREAME_CUBE_PREFIX));
            long end = Long.parseLong(resourceName.substring(resourceName.lastIndexOf("_")) + 1);
            String temp = resourceName.substring(RESOURCE_STREAME_CUBE_PREFIX.length(), resourceName.lastIndexOf("_"));
            long start = Long.parseLong(temp.substring(temp.lastIndexOf("_")) + 1);
            String cubeName = temp.substring(0, temp.lastIndexOf("_"));

            final Runnable runnable = new OneOffStreamingBuilder(cubeName, start, end).build();
            runnable.run();
        }

        @Transition(to = "STANDBY", from = "LEADER")
        public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
           

        }

        @Transition(to = "STANDBY", from = "OFFLINE")
        public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
           
        }


        @Transition(to = "OFFLINE", from = "STANDBY")
        public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
           
        }
    }
}
