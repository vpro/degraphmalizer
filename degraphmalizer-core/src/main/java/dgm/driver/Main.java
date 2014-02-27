package dgm.driver;

import dgm.driver.handler.HandlerModule;
import dgm.driver.server.Server;
import dgm.driver.server.ServerModule;
import dgm.fixtures.FixturesModule;
import dgm.fixtures.FixturesRunner;
import dgm.jmx.GraphBuilder;
import dgm.modules.BlueprintsSubgraphManagerModule;
import dgm.modules.DegraphmalizerModule;
import dgm.modules.ServiceRunner;
import dgm.modules.ThreadpoolModule;
import dgm.modules.elasticsearch.CommonElasticSearchModule;
import dgm.modules.elasticsearch.nodes.LocalES;
import dgm.modules.elasticsearch.nodes.NodeES;
import dgm.modules.fsmon.DynamicConfiguration;
import dgm.modules.fsmon.StaticConfiguration;
import dgm.modules.neo4j.CommonNeo4j;
import dgm.modules.neo4j.EmbeddedNeo4J;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.lang3.StringUtils;
import org.nnsoft.guice.sli4j.core.InjectLogger;
import org.nnsoft.guice.sli4j.slf4j.Slf4jLoggingModule;
import org.slf4j.Logger;

import com.beust.jcommander.JCommander;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

/* *
 * Run this with e.g
    -d -r -f /Users/michiel/vpro/degraphmalizer-config/trunk/src/main/conf/fixtures
 * to run the fixtures
 */
public final class Main {
    private static final String LOGBACK_CFG = "logback.configurationFile";

    @InjectLogger
    Logger log;

    private Main(String[] args) throws IOException {
        final Options opt = new Options();

        // parse CLI options
        final JCommander jcommander = new JCommander(opt, args);

        // print help and exit
        if (opt.help) {
            jcommander.usage();
            System.exit(1);
        }

        // find logback settings file
        if (System.getProperty(LOGBACK_CFG) == null) {
            System.setProperty(LOGBACK_CFG, opt.logbackConf);
        }

        // check if script directory exists
        if (!new File(opt.config).isDirectory()) {
            System.out.println("Cannot find configuration directory " + opt.config);
        }
        System.out.println("Configuration directory " + opt.config);
        System.out.println("Automatic configuration reloading: " + (opt.reloading ? "enabled" : "disabled"));

        // depending on properties / CLI, load proper modules
        final List<Module> modules = new ArrayList<Module>();

        // some defaults
        modules.add(new BlueprintsSubgraphManagerModule());
        modules.add(new Slf4jLoggingModule());
        modules.add(new DegraphmalizerModule());
        modules.add(new ThreadpoolModule());

        // netty part
        modules.add(new ServerModule(opt.bindhost, opt.port));
        modules.add(new HandlerModule());

        // we always run an embedded local graph database
        modules.add(new CommonNeo4j());
        modules.add(new EmbeddedNeo4J(opt.graphdb));

        // elasticsearch setup
        setupElasticsearch(opt, modules);

        // configuration reloading etc
        setupConfiguration(opt, modules);

        if (StringUtils.isNotEmpty(opt.fixtures)) {
            modules.add(new FixturesModule(opt.fixtures, opt.development, opt.reloading));
        }
        // the injector
        final Injector injector = Guice.createInjector(modules);

        // logger
        injector.injectMembers(this);

        // start JMX?
        if (opt.jmx) {
            // setup our JMX bean
            try {
                log.info("Starting JMX");
                final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
                final ObjectName name = new ObjectName("graph.mbeans:type=RandomizedGraphBuilder");
                final GraphBuilder gb = injector.getInstance(GraphBuilder.class);
                mbs.registerMBean(gb, name);
                log.info("JMX bean {} started", name);
            } catch (Exception e) {
                // TODO log errors
                e.printStackTrace();
            }
        }

        final Server server = injector.getInstance(Server.class);
        final ServiceRunner runner = injector.getInstance(ServiceRunner.class);

        // so we can shutdown cleanly
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("JVM Shutdown received (e.g., Ctrl-c pressed)");
                server.stopAndWait();

                runner.stopServices();
            }
        });

        // start services and then the main netty server
        runner.startServices();

        // start fixtures
        if (StringUtils.isNotEmpty(opt.fixtures)) {
            FixturesRunner fl = injector.getInstance(FixturesRunner.class);
            try {
                fl.runFixtures();
            } catch (Exception e) {
                log.error("Could not load fixtures. reason: {}", e.getMessage());
            }
        }


        log.info("Starting server on port {}", opt.port);
        server.startAndWait();
    }

    private void setupElasticsearch(Options opt, List<Module> modules) {
        modules.add(new CommonElasticSearchModule());

        // setup local node
        if (opt.development) {
            modules.add(new LocalES());
            return;
        }

        // setup node that connects to remote host
        if (opt.transport.size() != 3) {
            exit("You need to specify either the local or transport ES config. Exiting.");
        }

        final String cluster = opt.transport.get(2);
        final String host = opt.transport.get(0);
        final int port = Integer.parseInt(opt.transport.get(1));
        final String bindhost = opt.bindhost;

        log.info("Connecting to ES cluster:" + cluster + " bindhost: " + bindhost + " host: " + host + " port:" + port);

        modules.add(new NodeES(cluster, bindhost, host, port));
    }

    private void setupConfiguration(Options opt,  List<Module> modules) throws IOException {
        // automatic reloading
        if (opt.reloading) {
            modules.add(new DynamicConfiguration(opt.config, opt.libraries()));
        } else {
            modules.add(new StaticConfiguration(opt.config, opt.libraries()));
        }
    }

    public static void main(String[] args) throws IOException {
        new Main(args);
    }

    private static void exit(String message) {
        System.err.println(message);
        System.exit(1);
    }

}
