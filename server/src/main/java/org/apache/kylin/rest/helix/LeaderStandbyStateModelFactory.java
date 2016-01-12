package org.apache.kylin.rest.helix;

import org.apache.helix.NotificationContext;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class LeaderStandbyStateModelFactory extends StateTransitionHandlerFactory<TransitionHandler> {
    private static final Logger logger = LoggerFactory.getLogger(LeaderStandbyStateModelFactory.class);
    
    @Override
    public TransitionHandler createStateTransitionHandler(PartitionId partitionId) {
        if (partitionId.getResourceId().equals(ResourceId.from(HelixClusterAdmin.RESOURCE_NAME_JOB_ENGINE))) {
            return new JobEngineStateModel();
        }
        
        return null;
    }

    public static class JobEngineStateModel extends TransitionHandler {

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
}
