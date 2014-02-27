/*
 * Copyright (C) 2013 All rights reserved
 * VPRO The Netherlands
 */
package dgm.fixtures;

import dgm.configuration.*;
import dgm.trees.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Provider;

/**
 * User: rico
 * Date: 03/04/2013
 */
public class VerifyResultDocumentsCommand implements Command<List<Pair<String, Boolean>>> {
    private final Client client;
    private final Provider<Configuration> cfgProvider;
    private final Provider<FixtureConfiguration> fixtureConfigurationProvider;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(VerifyResultDocumentsCommand.class);

    @Inject
    public VerifyResultDocumentsCommand(Client client, Provider<Configuration> cfgProvider, Provider<FixtureConfiguration> fixtureConfiguration) {
        this.client = client;
        this.cfgProvider = cfgProvider;
        this.fixtureConfigurationProvider = fixtureConfiguration;
    }

    @Override
    public List<Pair<String, Boolean>> execute() {
        List<Pair<String, Boolean>> ids = new ArrayList<Pair<String, Boolean>>();
        try {
            FixtureConfiguration fixtureConfiguration = fixtureConfigurationProvider.get();
            for (String index : fixtureConfiguration.getIndexNames()) {
                for (String type : fixtureConfiguration.getIndexConfig(index).getTypeNames()) {
                    LOG.info("Checking " + index + ":" + type);
                    final Iterable<TypeConfig> configs = Configurations.configsFor(cfgProvider.get(), index, type);
                    for (TypeConfig typeConfig : configs) {
                        LOG.info("  Target " + typeConfig.targetIndex() + ":" + typeConfig.targetType());
                        ids.addAll(verifyDocuments(typeConfig.targetIndex(), typeConfig.targetType()));
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Something went wrong verifying fixture result documents.", e);
        }
        return ids;
    }

    private List<Pair<String, Boolean>> verifyDocuments(String indexName, String typeName) throws IOException, ExecutionException, InterruptedException {
        List<Pair<String, Boolean>> ids = new ArrayList<Pair<String, Boolean>>();


        FixtureTypeConfiguration fixtureTypeConfiguration =  fixtureConfigurationProvider.get().getExpectedIndexConfig(indexName).getTypeConfig(typeName);
        if (fixtureTypeConfiguration == null) {
            throw new RuntimeException("Could not find fixturesConfiguration for " + indexName + "/" + typeName);
        }
        for (String documentId : fixtureTypeConfiguration.getDocumentIds()) {
            String id = indexName + ":" + typeName + ":" + documentId;
            LOG.debug("Verifying document:  {}", id);

            Pair<String, Boolean> result = new Pair<String, Boolean>(id, verifyDocument(documentId, typeName, indexName));
            ids.add(result);
        }
        return ids;
    }

    private boolean verifyDocument(String id, String type, String index) throws IOException, ExecutionException, InterruptedException {
        FixtureIndexConfiguration expectedIndexConfig = fixtureConfigurationProvider.get().getExpectedIndexConfig(index);

        if (expectedIndexConfig == null) {
            LOG.error("No expected index configuration found for : {}", index);
            return false;
        }
        FixtureTypeConfiguration typeConfig = expectedIndexConfig.getTypeConfig(type);
        if (typeConfig == null) {
            LOG.error("No expected type configuration found for : {}/{} ", index, type);
            return false;
        }
        JsonNode documentExpected = typeConfig.getDocumentById(id);
        if (documentExpected == null) {
            LOG.warn("No expected document found for : {}/{}/{} ", new Object[]{index, type, id});
            return false;
        }
        JsonNode documentInIndex = getDocument(id, type, index);
        if (documentInIndex == null) {
            LOG.warn("No document found in index for : {}/{}/{} ", new Object[]{index, type, id});
            return false;
        }
        if (!documentExpected.equals(documentInIndex)) {
            LOG.warn("Documents are NOT equal for id {}/{}/{} ", new Object[]{index, type, id});
            LOG.warn(documentExpected.toString() + "!=" + documentInIndex.toString());
            return false;
        }
        return true;
    }

    private JsonNode getDocument(String id, String type, String index) throws ExecutionException, InterruptedException, IOException {
        try {


            final GetResponse response = client.prepareGet(index, type, id).execute().get();

            if (!response.exists())
                return null;

            return objectMapper.readTree(response.getSourceAsString());
        } catch (Exception e) {
            LOG.error(id + ":" + type + ":" + index + ":" + e.getMessage(), e);
            return null;
        }
    }
}
