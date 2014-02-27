package dgm.trees2;

import dgm.trees.*;

import java.util.Iterator;

import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;

import static org.fest.assertions.Assertions.assertThat;


@Test
public class MinimalTreeTest {
    // complete graph on 3 vertices, but then directed
    public static Pair<Graph, Vertex> K_3() {
        final TinkerGraph g = new TinkerGraph();

        final Vertex v1 = g.addVertex(null);
        final Vertex v2 = g.addVertex(null);
        final Vertex v3 = g.addVertex(null);

        g.addEdge(null, v1, v2, "this");
        g.addEdge(null, v2, v3, "is");
        g.addEdge(null, v3, v1, "cyclic");

        return new Pair<Graph, Vertex>(g, v1);
    }

    /* This graph:
     *
     * <pre>
     *
     *      ,-->(2)
     *    /
     * (1)---->(3) ---> (3_1)
     *   \
     *    `--->(4)
     *
     * </pre>
     */
    public static Pair<Graph, Vertex> forkGraph() {
        final TinkerGraph g = new TinkerGraph();

        final Vertex v1 = g.addVertex(null);
        final Vertex v2 = g.addVertex(null);
        final Vertex v3 = g.addVertex(null);
        final Vertex v4 = g.addVertex(null);
        final Vertex v3_1 = g.addVertex(null);

        g.addEdge(null, v1, v2, "a");
        g.addEdge(null, v1, v3, "b");
        g.addEdge(null, v1, v4, "c");
        g.addEdge(null, v3, v3_1, "d");

        return new Pair<Graph, Vertex>(g, v1);
    }

    static String nullSafeToString(final Object o) {
        if (o == null)
            return "null";

        return o.toString();
    }

    final static Function<Pair<Edge, Vertex>, String> show = new Function<Pair<Edge, Vertex>, String>() {
        @Override
        public String apply(Pair<Edge, Vertex> input) {
            return "(" + nullSafeToString(input.a) + "--" + nullSafeToString(input.b) + ')';
        }
    };


    @Test
    public void testTreeVisitors() {
        // 1 --> 2 --> 3 --> 1 --> 2 --> ...
        final Pair<Graph, Vertex> p = K_3();
        final Vertex root = p.b;

        final TreeViewer<Pair<Edge, Vertex>> treeViewer = new GraphTreeViewer(Direction.OUT);

        final PrettyPrinter<Pair<Edge, Vertex>> pp = new PrettyPrinter<Pair<Edge, Vertex>>(show);
        final LevelLimitingVisitor<Pair<Edge, Vertex>> llpp = new LevelLimitingVisitor<Pair<Edge, Vertex>>(5, pp);

        Trees2.bfsVisit(new Pair<Edge, Vertex>(null, root), treeViewer, llpp);

        final String s = "(null--v[0])\n" +
                "  (e[3][0-this->1]--v[1])\n" +
                "    (e[4][1-is->2]--v[2])\n" +
                "      (e[5][2-cyclic->0]--v[0])\n" +
                "        (e[3][0-this->1]--v[1])\n";

        assertThat(s).isEqualTo(pp.toString());


        final PrettyPrinter<Pair<Edge, Vertex>> pp2 = new PrettyPrinter<Pair<Edge, Vertex>>(show);
        final OccurrenceTracker<Pair<Edge, Vertex>> ot = new NodeAlreadyVisitedTracker();
        final CycleKiller<Pair<Edge, Vertex>> ckpp = new CycleKiller<Pair<Edge, Vertex>>(pp2, ot);

        Trees2.bfsVisit(new Pair<Edge, Vertex>(null, root), treeViewer, ckpp);

        final String t = "(null--v[0])\n" +
                "  (e[3][0-this->1]--v[1])\n" +
                "    (e[4][1-is->2]--v[2])\n";

        assertThat(t).isEqualTo(pp2.toString());
    }

    @Test
    public void testTreeBuilder() {
        final Pair<Graph, Vertex> p = forkGraph();
        final TreeViewer<Pair<Edge, Vertex>> tv = new GraphTreeViewer(Direction.OUT);
        final TreeBuilder<Pair<Edge, Vertex>> tb = new TreeBuilder<Pair<Edge, Vertex>>();

        Trees2.bfsVisit(new Pair<Edge, Vertex>(null, p.b), tv, tb);

        final Tree<Pair<Edge, Vertex>> tree = tb.tree();

        assertThat(tree.children()).hasSize(3);
        Iterator<TreeEntry<Pair<Edge, Vertex>>> i =  Trees.bfsWalk(tree).iterator();
        assertThat(i.next().getDistance()).isEqualTo(0);
        assertThat(i.next().getDistance()).isEqualTo(1);
        assertThat(i.next().getDistance()).isEqualTo(1);
        assertThat(i.next().getDistance()).isEqualTo(1);
        assertThat(i.next().getDistance()).isEqualTo(2);
    }
}
