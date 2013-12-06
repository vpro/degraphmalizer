/*
 * Copyright (C) 2013 All rights reserved
 * VPRO The Netherlands
 */
package dgm.fixtures;

import dgm.Degraphmalizr;
import dgm.ID;
import dgm.configuration.*;
import dgm.trees.Pair;

import java.util.HashSet;
import java.util.List;

import javax.inject.Inject;

import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.inject.Provider;

/**
 * User: rico
 * Date: 09/04/2013
 */
public class FixturesTestRunner implements ConfigurationMonitor, FixturesRunner {

    protected final Client client;
    protected final Provider<Configuration> cfgProvider;
    protected final Provider<FixtureConfiguration> fixtureConfigurationProvider;
    protected final RedegraphmalizeCommand redegraphmalizeCommand;
    protected final VerifyResultDocumentsCommand verifyResultDocumentsCommand;

    private static final Logger LOG = LoggerFactory.getLogger(FixturesDevelopmentRunner.class);

    @Inject
    public FixturesTestRunner(Client client, Provider<Configuration> cfgProvider, Provider<FixtureConfiguration> fixtureConfigurationProvider,  Degraphmalizr degraphmalizr) {
        this.client = client;
        this.cfgProvider = cfgProvider;
        this.fixtureConfigurationProvider = fixtureConfigurationProvider;

        redegraphmalizeCommand = new RedegraphmalizeCommand(client, cfgProvider, fixtureConfigurationProvider, degraphmalizr);
        verifyResultDocumentsCommand = new VerifyResultDocumentsCommand(client, cfgProvider, fixtureConfigurationProvider);
    }

    @Override
    public void runFixtures() {
        try {

            List<ID> ids = redegraphmalizeCommand.execute();
            LOG.info("Degraphmalized {} documents", ids.size());
            List<Pair<String, Boolean>> verifyResults = verifyResultDocumentsCommand.execute();
            LOG.info("Checked {} result documents", verifyResults.size());
            int success = 0;
            int failed = 0;
            for (Pair<String, Boolean> result : verifyResults) {
                if (result.b) {
                    success++;
                } else {
                    failed++;
                }
            }
            LOG.info("Verify results {} good, {} bad ", success, failed);
        } catch (Exception e) {
            LOG.error("Fixture run failed: {} ", e.getMessage());
            e.printStackTrace();
        }
    }


    @Override
    public void configurationChanged(String change) {
        //when the index that was just reloaded is the source index for one of the index configurations, we reinsert the
        //fixtures.
        final Configuration cfg = cfgProvider.get();

        // for quick lookup of the index name
        final HashSet<String> names = new HashSet<String>();
        Iterables.addAll(names, fixtureConfigurationProvider.get().getIndexNames());

        boolean needRun = false;
        for (IndexConfig indexConfig : cfg.indices().values())
            for (TypeConfig typeConfig : indexConfig.types().values()) {
                if (names.contains(typeConfig.sourceIndex())) {
                    needRun = true;
                    break;
                }

            }

        if (needRun)
            try {
                runFixtures();
            } catch (Exception e) {
                LOG.error("Something went wrong inserting the fixtures after a configurationChanged event.", e);
            }
    }
}


