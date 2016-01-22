package org.apache.kylin.rest.helix;

import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.kylin.common.KylinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kylin.rest.helix.HelixClusterAdmin.RESOURCE_STREAME_CUBE_PREFIX;

/**
 */
public class LeaderStandbyStateModelFactory extends StateTransitionHandlerFactory<TransitionHandler> {
    private final KylinConfig kylinConfig;

    public LeaderStandbyStateModelFactory(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    @Override
    public TransitionHandler createStateTransitionHandler(PartitionId partitionId) {
        if (partitionId.getResourceId().equals(ResourceId.from(HelixClusterAdmin.RESOURCE_NAME_JOB_ENGINE))) {
            return JobEngineTransitionHandler.getInstance(kylinConfig);
        }

        if (partitionId.getResourceId().stringify().startsWith(RESOURCE_STREAME_CUBE_PREFIX)) {
            return StreamCubeBuildTransitionHandler.getInstance(kylinConfig);
        }

        return null;
    }

}
