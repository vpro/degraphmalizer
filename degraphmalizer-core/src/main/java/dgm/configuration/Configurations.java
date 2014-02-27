package dgm.configuration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.google.common.base.Predicate;

public class Configurations {
    final static private Logger LOG = LoggerFactory.getLogger(Configurations.class);

    /**
     * Find all TypeConfigs with specified source index and source type
     */
    public static Iterable<TypeConfig> configsFor(Configuration cfg, String srcIndex, String srcType) {
        StringBuilder logMessage = null;

        if (LOG.isDebugEnabled()) {
            logMessage = new StringBuilder("Matching request for /");
            logMessage.append(srcIndex).append("/").append(srcType);
            logMessage.append(" to [");
        }

        final List<TypeConfig> configs = new ArrayList<TypeConfig>();

        // find all matching configs
        for (IndexConfig i : cfg.indices().values()) {
            for (TypeConfig t : i.types().values()) {
                if (srcIndex.equals(t.sourceIndex()) && (srcType == null || srcType.equals(t.sourceType()))) {
                    if (logMessage != null) {
                        logMessage.append(" /").append(t.targetIndex());
                        logMessage.append("/").append(t.targetType());
                        logMessage.append(", ");
                    }

                    configs.add(t);
                }
            }
        }
        if (logMessage != null) {
            LOG.debug(logMessage.append("]").toString());
        }

        return configs;
    }

    public static final Predicate<URL> JS = new Predicate<URL>() {
        @Override
        public boolean apply(URL url) {
            return url.getPath().endsWith(".js");
        }
    };


    //
    private static final PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

    public static List<URL> list(String directory, Predicate<URL> filter) throws IOException {
        List<URL> result = new ArrayList<URL>();
        if (! directory.endsWith("/")) {
            directory += "/";
        }
        try {
            for (Resource res : resolver.getResources(directory + "*")) {
                URL url = res.getURL();
                if (filter == null || filter.apply(url)) {
                    result.add(url);
                }
            }
        } catch (FileNotFoundException ignored) {

        }
        return result;
    }


    public static List<String> listDirectories(String directory) throws IOException {
        List<String> result = new ArrayList<String>();
        if (!directory.endsWith("/")) {
            directory += "/";
        }
        try {
            for (Resource res : resolver.getResources(directory + "*/")) {
                result.add(directory + res.createRelative(".").getFilename() + "/");
            }
        } catch (FileNotFoundException ignored) {

        }
        try {
            for (Resource res : resolver.getResources(directory + "*")) {
                result.add(directory + res.getFilename() + "/");
            }
        } catch (FileNotFoundException ignored) {

        }
        return result;
    }


    public static List<URL> load(final String... resources) throws IOException {
        List<URL> result = new ArrayList<URL>();
        for (String r : resources) {
            result.add(resolver.getResource(r).getURL());
        }
        return result;
    }

}
