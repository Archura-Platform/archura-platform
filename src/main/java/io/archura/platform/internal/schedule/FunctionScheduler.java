package io.archura.platform.internal.schedule;

import org.quartz.SchedulerException;
import org.quartz.core.QuartzScheduler;
import org.quartz.core.QuartzSchedulerResources;

public class FunctionScheduler extends QuartzScheduler {
    /**
     * <p>
     * Create a <code>QuartzScheduler</code> with the given configuration
     * properties.
     * </p>
     *
     * @param resources
     * @param idleWaitTime
     * @param dbRetryInterval
     * @see QuartzSchedulerResources
     */
    public FunctionScheduler(QuartzSchedulerResources resources, long idleWaitTime, long dbRetryInterval) throws SchedulerException {
        super(resources, idleWaitTime, dbRetryInterval);
    }

    @Override
    public ThreadGroup getSchedulerThreadGroup() {
        return Thread.ofVirtual().start(() -> {
        }).getThreadGroup();
    }

}
