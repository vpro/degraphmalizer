package elasticsearch;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import static org.elasticsearch.node.NodeBuilder.*;

/**
 * Configure local ES
 */
public class LocalES extends AbstractModule
{
    @Override
    protected void configure()
    {}

    @Provides
    @Singleton
    final Client provideElasticInterface()
    {
        final Settings.Builder settings = ImmutableSettings.settingsBuilder()
                //.put("path.data", dataDir.getAbsolutePath())
                .put("node.http.enabled", true)
                .put("index.store.type", "memory")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0);

        final Node node = NodeBuilder.nodeBuilder().local(true).settings(settings).node();

        return node.client();
    }
}
