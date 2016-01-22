package org.apache.kylin.rest.helix;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.impl.threadpool.DefaultScheduler;
import org.apache.kylin.job.lock.MockJobLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentMap;

/**
 */
public class JobEngineTransitionHandler extends TransitionHandler {
    private static final Logger logger = LoggerFactory.getLogger(JobEngineTransitionHandler.class);
    private final KylinConfig kylinConfig;

    private static ConcurrentMap<KylinConfig, JobEngineTransitionHandler> instanceMaps = Maps.newConcurrentMap();

    private JobEngineTransitionHandler(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    public static JobEngineTransitionHandler getInstance(KylinConfig kylinConfig) {
        Preconditions.checkNotNull(kylinConfig);
        instanceMaps.putIfAbsent(kylinConfig, new JobEngineTransitionHandler(kylinConfig));
        return instanceMaps.get(kylinConfig);
    }

    @Transition(to = "LEADER", from = "STANDBY")
    public void onBecomeLeaderFromStandby(Message message, NotificationContext context) {
        logger.info("JobEngineStateModel.onBecomeLeaderFromStandby()");
        try {
            DefaultScheduler scheduler = DefaultScheduler.createInstance();
            scheduler.init(new JobEngineConfig(this.kylinConfig), new MockJobLock());
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
