package elasticsearch;

import com.google.common.io.Files;
import com.google.inject.*;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.File;

/**
 * Configure local ES
 */
public class EmphemeralES extends AbstractModule
{
    @Override
    protected void configure()
    {}

    @Provides
    @Singleton
    final Client provideElasticInterface()
    {
        // create temporary directory and use this as ES datadir
        final File dataDir = Files.createTempDir();

        // otherwise run as much as possible in memory
        final Settings.Builder settings = ImmutableSettings.settingsBuilder()
                .put("path.data", dataDir.getAbsolutePath())
                .put("node.http.enabled", false)
                .put("gateway.type", "none")
                .put("index.store.type", "memory")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0);

        final Node node = NodeBuilder.nodeBuilder().local(true).settings(settings).node();

        return node.client();
    }
}
