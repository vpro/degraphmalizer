package dgm.modules.elasticsearch;

import com.google.common.base.Optional;
import org.elasticsearch.action.get.GetResponse;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class ResolvedPathElement
{
    private final Optional<GetResponse> getResponse;
    private final Edge edge;
    private final Vertex vertex;

    public ResolvedPathElement(Optional<GetResponse> getResponse, Edge edge, Vertex vertex)
    {
        this.getResponse = getResponse;
        this.edge = edge;
        this.vertex = vertex;
    }

    public final Optional<GetResponse> getResponse()
    {
        return getResponse;
    }

    public final Edge edge()
    {
        return edge;
    }

    public final Vertex vertex()
    {
        return vertex;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ResolvedPathElement resolvedPathElement = (ResolvedPathElement) o;

        if (!edge.equals(resolvedPathElement.edge)) return false;
        if (!getResponse.equals(resolvedPathElement.getResponse)) return false;
        if (!vertex.equals(resolvedPathElement.vertex)) return false;

        return true;
    }

    @Override
    public final int hashCode() {
        int result = getResponse.hashCode();
        result = 31 * result + edge.hashCode();
        result = 31 * result + vertex.hashCode();
        return result;
    }

    @Override
    public final String toString() {
        return "ResolvedPathElement{" +
                "getResponse=" + getResponse +
                ", edge=" + edge +
                ", vertex=" + vertex +
                '}';
    }
}
