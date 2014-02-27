/*
 * Copyright (C) 2013 All rights reserved
 * VPRO The Netherlands
 */
package dgm.configuration.javascript;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import dgm.JSONUtilities;
import dgm.modules.elasticsearch.ResolvedPathElement;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Scriptable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * User: rico
 * Date: 26/04/2013
 */
public class JavascriptNode
{
    final Logger log = LoggerFactory.getLogger(JavascriptNode.class);

    final ResolvedPathElement resolvedPathElement;
    final Scriptable scope;


    public JavascriptNode(Scriptable scope, ResolvedPathElement resolvedPathElement)
    {
        this.resolvedPathElement = resolvedPathElement;
        this.scope = scope;
    }

    public Edge getEdge()
    {
        return resolvedPathElement.edge();
    }

    public Vertex getVertex()
    {
        return resolvedPathElement.vertex();
    }

    public boolean getExists()
    {
        return resolvedPathElement.getResponse().isPresent();
    }

    public Object getDocument() throws Exception
    {
        try
        {
            Context context = Context.enter();
            if (getExists())
            {
                final String getResponseString = resolvedPathElement.getResponse().get().getSourceAsString();

                return JSONUtilities.toJSONObject(context, scope, getResponseString);
            }
        } catch (Exception e) {
            log.error("Exception retrieving document {} ", new Object[] {e.getMessage() , e });
            throw(e);
        } finally {
            Context.exit();
        }
        return null;
    }
}
