package degraphmalizr.degraphmalize;

import com.fasterxml.jackson.databind.JsonNode;
import configuration.TypeConfig;
import degraphmalizr.ID;

import java.util.concurrent.Future;

public class DegraphmalizeAction
{
    protected final DegraphmalizeActionType actionType;

    protected final TypeConfig typeCfg;
    protected final ID id;

    protected JsonNode document = null;
    protected final DegraphmalizeStatus status;

    // TODO fix in Degraphmalizer.degraphmalizeJob
    public Future<JsonNode> result;

    public DegraphmalizeAction(DegraphmalizeActionType actionType, ID id, TypeConfig typeCfg, DegraphmalizeStatus callback)
    {
        this.actionType = actionType;
        this.id = id;
        this.typeCfg = typeCfg;
        this.status = callback;
    }

    public final ID id()
    {
        return id;
    }

    public DegraphmalizeActionType type() {
        return actionType;
    }

    public final TypeConfig typeConfig()
    {
        return typeCfg;
    }

    public final DegraphmalizeStatus status()
    {
        return status;
    }

    // TODO wrong
    public final void setDocument(JsonNode document)
    {
        this.document = document;
    }

    public final JsonNode document()
    {
        return document;
    }

    public final Future<JsonNode> resultDocument()
    {
        return result;
    }
}
