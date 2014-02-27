package dgm.trees;

import com.tinkerpop.blueprints.*;
import dgm.GraphUtilities;

/**
 * A pair of values
 * 
 * @author wires
 *
 * @param <A>
 * @param <B>
 */
public class Pair<A,B>
{
	public final A a;
	public final B b;
	
	public Pair(A a, B b)
	{
		this.a = a;
		this.b = b;
	}

    @Override
	public final String toString()
	{
        return "<" + getStringRepresentation(a) + "," + getStringRepresentation(b) + ">";
	}
	
	// TODO it's a hack...
	private static String getStringRepresentation(Object x)
	{
        if(x == null)
            return "null";

		if (x instanceof Vertex)
		{
			final Object value = ((Vertex)x).getProperty(GraphUtilities.IDENTIFIER);
			return value != null ? value.toString().trim() : x.toString();
		}

		if (x instanceof Edge)
		{
			final Edge e = (Edge)x;
			String stringRepresentation = getStringRepresentation(e.getVertex(Direction.OUT)) + "---[" + e.getLabel() + "]--->" + getStringRepresentation(e.getVertex(Direction.IN));
            return stringRepresentation.trim();
		}

		return x.toString().trim();
	}
}
