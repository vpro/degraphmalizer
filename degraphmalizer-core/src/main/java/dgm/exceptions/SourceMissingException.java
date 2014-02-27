package dgm.exceptions;

import dgm.ID;

/**
 * Nodes were found in the graph with a version that cannot be found in Elasticsearch.
 *
 */
public class SourceMissingException extends DegraphmalizerException {
    final protected ID id;

    public SourceMissingException(ID id) {
        super("Source document of " + id + " could not be found in ES");
        this.id  = id;
    }

    /**
     * Get the ID that couldn't be found.
     */
    public ID id() {
        return id;
    }
}
