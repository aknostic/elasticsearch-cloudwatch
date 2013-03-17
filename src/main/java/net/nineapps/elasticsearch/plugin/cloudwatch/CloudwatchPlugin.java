package net.nineapps.elasticsearch.plugin.cloudwatch;

import static org.elasticsearch.common.collect.Lists.newArrayList;

import java.util.Collection;

import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;

public class CloudwatchPlugin extends AbstractPlugin {

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
    public Collection<Class<? extends LifecycleComponent>> services() {
		Collection<Class<? extends LifecycleComponent>> services = newArrayList();
        if (settings.getAsBoolean("metrics.cloudwatch.enabled", true)) {
            services.add(CloudwatchPluginService.class);
        }
        return services;
    }}
