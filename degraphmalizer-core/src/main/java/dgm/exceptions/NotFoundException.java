package dgm.exceptions;

import dgm.ID;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Nodes were found in the graph that cannot be found in Elasticsearch.
 *
 */
public class NotFoundException extends DegraphmalizerException
{
    final Iterable<ID> missing;

    public NotFoundException(Iterable<ID> missing)
    {
        super(exceptionMessage(missing));
        this.missing = missing;
    }

    public NotFoundException(ID... missing)
    {
        this(Arrays.asList(missing));
    }

    private static String exceptionMessage(Iterable<ID> missing)
    {
        final StringBuilder sb = new StringBuilder("Documents not found in ES: ");

        // id1; id2; id3 ...
        final Iterator<ID> ids = missing.iterator();
        while(ids.hasNext())
        {
            final ID id = ids.next();
            sb.append(id.toString());
            if(ids.hasNext())
                sb.append("; ");
        }

        return sb.toString();
    }

    /**
     * Get a list of missing ID's.
     *
     * This means that the ID in the graph can not be found in elasticsearch.
     */
    public Iterable<ID> missing()
    {
        return missing;
    }
}
