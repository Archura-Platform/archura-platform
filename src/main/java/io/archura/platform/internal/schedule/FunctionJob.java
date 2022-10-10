package io.archura.platform.internal.schedule;

import io.archura.platform.api.context.Context;
import io.archura.platform.api.type.functionalcore.ContextConsumer;
import io.archura.platform.external.FilterFunctionExecutor;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;

public class FunctionJob implements Job {
    @Override
    public void execute(final JobExecutionContext jobExecutionContext) {
        final JobDataMap jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        final FilterFunctionExecutor filterFunctionExecutor = (FilterFunctionExecutor) jobDataMap.get(FilterFunctionExecutor.class.getSimpleName());
        final Context context = (Context) jobDataMap.get(Context.class.getSimpleName());
        final ContextConsumer contextConsumer = (ContextConsumer) jobDataMap.get(ContextConsumer.class.getSimpleName());
        filterFunctionExecutor.execute(context, contextConsumer);
    }
}
