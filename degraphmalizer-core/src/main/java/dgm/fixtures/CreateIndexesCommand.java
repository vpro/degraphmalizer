/*
 * Copyright (C) 2013 All rights reserved
 * VPRO The Netherlands
 */
package dgm.fixtures;

import dgm.configuration.Configuration;
import dgm.configuration.FixtureConfiguration;
import dgm.configuration.FixtureIndexConfiguration;
import dgm.configuration.FixtureTypeConfiguration;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Provider;

/**
 * User: rico
 * Date: 08/04/2013
 */
public class CreateIndexesCommand implements Command<List<String>> {
    private final Client client;
    private final Provider<FixtureConfiguration> fixtureConfigurationProvider;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(CreateIndexesCommand.class);

    @Inject
    public CreateIndexesCommand(Client client, Provider<Configuration> cfgProvider, Provider<FixtureConfiguration> fixtureConfigurationProvider) {
        this.client = client;
        this.fixtureConfigurationProvider = fixtureConfigurationProvider;
    }

    @Override
    public List<String> execute() throws Exception {
        List<String> indexes = new ArrayList<String>();
        FixtureConfiguration fixtureConfig = fixtureConfigurationProvider.get();
        for (String indexName : fixtureConfig.getIndexNames()) {
            LOG.debug("Creating index [{}]", indexName);
            FixtureIndexConfiguration indexConfig = fixtureConfig.getIndexConfig(indexName);
            try {
                client.admin().indices().create(buildCreateIndexRequest(indexName, indexConfig)).get();
                indexes.add(indexName);
            } catch (Exception e) {
                throw new Exception("something went wrong creating index [" + indexName + "]", e);
            }
        }
        return indexes;
    }

    CreateIndexRequest buildCreateIndexRequest(String indexName, FixtureIndexConfiguration indexConfig) {
        CreateIndexRequest request = new CreateIndexRequest(indexName, createSettings(indexConfig));
        for (String typeName : indexConfig.getTypeNames()) {
            FixtureTypeConfiguration typeConfig = indexConfig.getTypeConfig(typeName);
            if (typeConfig.getMapping() != null) {
                request.mapping(typeName, typeConfig.getMapping().toString());
                LOG.debug("Add mapping for type [{}] in index [{}]", typeName, indexName);
            }
        }
        return request;
    }

    private Settings createSettings(FixtureIndexConfiguration indexConfig) {

        Settings settings = indexConfig.getSettingsConfig();
        if (settings == null) {
            ObjectNode indexConfigSettingsNode = objectMapper.createObjectNode();
            indexConfigSettingsNode.
                    put("number_of_shards", 2).
                    put("number_of_replicas", 1);

            return ImmutableSettings.settingsBuilder().loadFromSource(indexConfigSettingsNode.toString()).build();
        } else {
            return settings;
        }
    }
}
