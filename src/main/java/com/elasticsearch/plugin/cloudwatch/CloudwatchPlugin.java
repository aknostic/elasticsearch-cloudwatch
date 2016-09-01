package com.elasticsearch.plugin.cloudwatch;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;

public class CloudwatchPlugin extends Plugin {

    private final Settings settings;

    public CloudwatchPlugin(Settings settings) {
        this.settings = settings;
    }

    public String name() {
        return "cloudwatch-plugin";
    }

    public String description() {
        return "Plugin which stores cluster state stats in Cloudwatch";
    }


    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<Class<? extends LifecycleComponent>>();
        if (settings.getAsBoolean("metrics.cloudwatch.enabled", true)) {
            services.add(CloudwatchPluginService.class);
        }
        return services;
    }
}
