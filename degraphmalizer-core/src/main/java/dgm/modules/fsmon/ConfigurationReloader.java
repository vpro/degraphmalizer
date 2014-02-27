package dgm.modules.fsmon;

import dgm.configuration.*;
import dgm.exceptions.ConfigurationException;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;


@Singleton
class ConfigurationReloader implements FilesystemMonitor, Provider<Configuration> {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationReloader.class);


    private final CachedProvider<Configuration> cachedProvider;

    private final ConfigurationMonitor configurationMonitor;

    private final static ObjectMapper OM = new ObjectMapper();

    @Inject
    public ConfigurationReloader(Set<ConfigurationMonitor> configurationMonitors,
                                 final @Named("scriptFolder") String scriptFolder,
                                 final @Named("libraryFiles") List<URL> libraries) throws IOException {
        LOG.info("Starting with {} {} ", scriptFolder, libraries);


        configurationMonitor = new CompositeConfigurationMonitor(configurationMonitors);

        /**
         * When loading the configuration this will throw an exception with an invalid configuration, but only on startup.
         * When already running with a configuration this will LOG an error message trying to load an invalid configuration, but
         * will continue to run with the previous valid configuration.
         */
        final Provider<Configuration> confLoader = new Provider<Configuration>() {
            private final AtomicReference<Configuration> configuration =
                new AtomicReference<Configuration>(AbstractConfigurationModule.createConfiguration(OM, scriptFolder, libraries));

            @Override
            public Configuration get() {
                try {
                    final Configuration d = configuration.getAndSet(null);
                    if (d == null)
                        return AbstractConfigurationModule.createConfiguration(OM, scriptFolder, libraries);

                    return d;
                } catch (ConfigurationException ce) {
                    LOG.info("Failed to load configuration, {}", ce.getMessage());
                    return null;
                } catch (Exception e) {
                    LOG.info("Unknown Exception while loading configuration, {}", e);
                    return null;
                }
            }
        };

        this.cachedProvider = new CachedProvider<Configuration>(confLoader);
    }

    @Override
    public void directoryChanged(String directory) {
        LOG.info("Filesystem change detected for directory (target-index) {}", directory);

        // try to reload the configuration
        if (!cachedProvider.invalidate()) {
            LOG.info("Failed to reload configuration");
        }

        // print configuration if debugging is enabled
        if (LOG.isDebugEnabled()) {
            // get new config
            final Configuration cfg = cachedProvider.get();
            for (final IndexConfig i : cfg.indices().values()) {
                for (final TypeConfig t : i.types().values()) {
                    LOG.debug("Found target configuration /{}/{} --> /{}/{}",
                        new Object[]{t.sourceIndex(), t.sourceType(), i.name(), t.name()});
                }
            }
        }

        // notify all configuration listeners
        configurationMonitor.configurationChanged(directory);
    }

    @Override
    public Configuration get() {
        return cachedProvider.get();

    }
}
