package dgm.configuration;


import com.fasterxml.jackson.databind.JsonNode;

/**
 * @author Ernst Bunders
 */
public interface FixtureTypeConfiguration {
    JsonNode getMapping();
    Iterable<JsonNode> getDocuments();
    Iterable<String>getDocumentIds();
    JsonNode getDocumentById(String id);
    boolean hasDocuments();

}
