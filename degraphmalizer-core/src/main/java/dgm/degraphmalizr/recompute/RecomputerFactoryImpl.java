package dgm.degraphmalizr.recompute;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import dgm.GraphUtilities;
import dgm.ID;
import dgm.configuration.PropertyConfig;
import dgm.configuration.TypeConfig;
import dgm.configuration.WalkConfig;
import dgm.exceptions.*;
import dgm.modules.bindingannotations.Fetches;
import dgm.modules.bindingannotations.Recomputes;
import dgm.modules.elasticsearch.DocumentProvider;
import dgm.modules.elasticsearch.QueryFunction;
import dgm.modules.elasticsearch.ResolvedPathElement;
import dgm.trees.Pair;
import dgm.trees.Tree;
import dgm.trees.TreeEntry;
import dgm.trees.Trees;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.nnsoft.guice.sli4j.core.InjectLogger;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static dgm.GraphUtilities.toJSON;


public class RecomputerFactoryImpl implements Recomputer {
    @InjectLogger
    Logger log;

    protected final Client client;
    protected final Graph graph;
    protected final ExecutorService recomputeQueue;
    protected final ExecutorService fetchQueue;
    protected final QueryFunction queryFn;
    protected final ObjectMapper objectMapper;
    protected final DocumentProvider documentProvider;

    @Inject
    public RecomputerFactoryImpl(Client client, Graph graph,
                                 @Fetches ExecutorService fetchQueue,
                                 @Recomputes ExecutorService recomputeQueue,
                                 ObjectMapper objectMapper,
                                 QueryFunction queryFunction, DocumentProvider documentProvider) {
        this.fetchQueue = fetchQueue;
        this.recomputeQueue = recomputeQueue;
        this.graph = graph;
        this.client = client;
        this.queryFn = queryFunction;
        this.objectMapper = objectMapper;
        this.documentProvider = documentProvider;
    }

    class Recomputer {
        protected final RecomputeRequest request;
        protected final RecomputeCallback callback;


        public Recomputer(RecomputeRequest request, RecomputeCallback callback) {
            this.request = request;
            this.callback = callback;
        }

        private Map<String, JsonNode> walkResults() throws ExecutionException, InterruptedException {
            final Map<String, JsonNode> walkResults = new HashMap<String, JsonNode>();

            if (request.config.walks().entrySet().isEmpty()) {
                return walkResults;
            }

            boolean isAbsent = false;
            Tree<Pair<Edge, Vertex>> absentTree = null;

            for (Map.Entry<String, WalkConfig> walkCfg : request.config.walks().entrySet()) {
                // walk graph, and fetch all the children in the opposite direction of the walk
                final Tree<Pair<Edge, Vertex>> tree =
                        GraphUtilities.childrenFrom(request.root.vertex(), walkCfg.getValue().direction());

                // write size information to log
                if (log.isDebugEnabled()) {
                    final int size = Iterables.size(Trees.bfsWalk(tree));
                    log.debug("Retrieving {} documents from ES", size);
                }

                // get all documents in the tree from Elasticsearch (in parallel)
                final Tree<Optional<ResolvedPathElement>> docTree = Trees.pmap(fetchQueue, queryFn, tree);

                // if some value is absent from the tree, abort the computation
                final Optional<Tree<ResolvedPathElement>> fullTree = Trees.optional(docTree);

                // TODO this does not work at present as Trees.optional behaves 'lazy'
                if (!fullTree.isPresent()) {
                    isAbsent = true;
                    absentTree = tree;
                    break;
                }

                // reduce each property to a value based on the walk result
                for (final Map.Entry<String, ? extends PropertyConfig> propertyCfg : walkCfg.getValue().properties().entrySet()) {
                    try {
                        walkResults.put(propertyCfg.getKey(), propertyCfg.getValue().reduce(fullTree.get()));
                    } catch (ValueIsAbsentException v) {
                        isAbsent = true;
                        absentTree = tree;
                        break;
                    }
                }
            }

            // something failed, so we abort the whole re-computation
            if (isAbsent) {
                log.debug("Some results were absent, aborting re-computation for {}", request.root.id());

                // TODO return list of expired nodes/IDs
                Collection<ID> ids = computeAbsentIDs(absentTree);
                log.info("Aborted recompute for {} because graph is incomplete for this node", request.root.id().toString());
                throw new ExpiredException(ids);

            }

            return walkResults;
        }

        private IndexResponse writeToES(ObjectNode document) {
            final TypeConfig conf = request.config;
            final ID sourceID = request.root.id();
            final ID targetID = sourceID.index(conf.targetIndex()).type(conf.targetType());

            // write the source version to the document
            document.put("_fromSource", toJSON(objectMapper, sourceID));
            final String documentSource = document.toString();

            // write document to Elasticsearch
            final IndexResponse ir = client.prepareIndex(targetID.index(), targetID.type(), targetID.id())
                    .setSource(documentSource).execute().actionGet();

            // log some stuff
            if (log.isDebugEnabled()) {
                log.debug("Written /{}/{}/{}, version={}", new Object[]{targetID.index(), targetID.type(), targetID.id(), ir.version()});
                log.debug("Content: {}", documentSource);
            }

            return ir;
        }

        private JsonNode getFromES() throws IOException {
            // TODO oops this doesn't work at the moment, we have the REDUCE results in walkResults :(
            //            // we are always on the root node of a walk result, so use that if it's there
            //            if(walkResults != null && !walkResults.isEmpty())
            //                return walkResults.values().iterator().next();

            // when no walks are defined, we just get the document ourselves.

            // TODO handle this properly...
            //todo: what if Optional.absent??
            //todo: queryFn.apply may produce null

            // retrieve the raw document from ES
            final Optional<ResolvedPathElement> r = queryFn.apply(new Pair<Edge, Vertex>(null, request.root.vertex()));
            if (!r.isPresent() || !r.get().getResponse().isPresent())
                throw new SourceMissingException(request.root.id());

            return objectMapper.readTree(r.get().getResponse().get().sourceAsString());
        }

        public RecomputeResult recompute() throws IOException, ExecutionException, InterruptedException {
            log.debug("Recompute {} started", request.root.id().toString());

            // Now we are going to:
            // - fetch the current ElasticSearch document,
            final JsonNode rawDocument = getFromES();

            // - Return when this document does not need to be processed.
            if (!request.config.filter(rawDocument)) {
                log.debug("Aborted recompute for {} because filter=false for this document", request.root.id().toString());
                throw new DocumentFiltered();
            }

            // Now we are going to iterate over all the walks configured for this input document. For each walk:
            // - We fetch a tree of children non-recursively from our document in the inverted direction of the walk, as Graph vertices
            // - We convert the tree of vertices to a tree of ElasticSearch documents
            // - We call the reduce() method for this walk, with the tree of documents as argument.
            // - We collect the result.
            final Map<String, JsonNode> walkResults = walkResults();
            if (walkResults == null) {
                log.info("Aborted recompute for {} because graph is expired for this node", request.root.id().toString());
                throw new ExpiredException(Collections.<ID>emptyList());
            }

            // Now we are going to:
            // - Transform it, if transformation is required
            // - Add the walk properties
            // - Add a reference to the source document.
            // - And store it as target document type in target index.

            // pre-process document using javascript
            final JsonNode transformed = request.config.transform(rawDocument);

            if (!transformed.isObject()) {
                log.info("Aborted recompute for {} because the source document is not a JSON object", request.root.id().toString());
                throw new SourceNotObjectException();
            }

            final ObjectNode document = (ObjectNode) transformed;

            // add the results to the document
            for (Map.Entry<String, JsonNode> e : walkResults.entrySet()) {
                document.put(e.getKey(), e.getValue());
            }

            // write the result document to the target index
            final IndexResponse ir = writeToES(document);

            log.debug("Recompute completed for {}, wrote /{}/{}/{}/{}",
                    new Object[]{request.root.id().toString(), ir.index(), ir.type(), ir.id(), ir.version()});

            return new RecomputeResult(ir, rawDocument, document, walkResults);
        }
    }

    private Collection<ID> computeAbsentIDs(Tree<Pair<Edge, Vertex>> absentTree) {
        List<ID> ids = new ArrayList<ID>();
        final Tree<DocumentResult> docTree;
        try {
            docTree = Trees.pmap(fetchQueue, new Function<Pair<Edge, Vertex>, DocumentResult>() {
                @Override
                public DocumentResult apply(Pair<Edge, Vertex> pair) {
                    final ID id = GraphUtilities.getID(objectMapper, pair.b);
                    if (id == null) {
                        return new DocumentResult(id, DocumentState.NODOCUMENT);
                    }

                    if (id.version() == 0) {
                        return new DocumentResult(id, DocumentState.SYMBOLIC);
                    }

                    GetResponse r = documentProvider.get(id);

                    if ((r.version() == -1) || !r.exists()) {
                        return new DocumentResult(id, DocumentState.NOTFOUND);
                    }

                    if (r.version() != id.version()) {
                        return new DocumentResult(id, DocumentState.EXPIRED);
                    }

                    return new DocumentResult(id, DocumentState.FOUND);
                }
            }, absentTree);

            for (TreeEntry<DocumentResult> treeEntry : Trees.bfsWalk(docTree)) {
                DocumentResult rpe = treeEntry.getValue();
                if (rpe.getState()==DocumentState.EXPIRED) {
                    ids.add(rpe.getId());
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ids;
    }

    private class DocumentResult {
        ID id;
        DocumentState state;

        private DocumentResult(ID id, DocumentState state) {
            this.id = id;
            this.state = state;
        }

        public ID getId() {
            return id;
        }

        public void setId(ID id) {
            this.id = id;
        }

        public DocumentState getState() {
            return state;
        }

        public void setState(DocumentState state) {
            this.state = state;
        }
    }

    private enum DocumentState {
        NOTFOUND,
        EXPIRED,
        FOUND,
        SYMBOLIC,
        NODOCUMENT
    }

    /**
     * This procuedure actually performes the recompute of individual documents. It performs transformation,
     * applies walks and inserts/updates the target document.
     *
     * @param request represents the source document and recompute configuration.
     * @return the ES IndexRespons to the insert of the target document.
     */
    @Override
    public RecomputeResult recompute(final RecomputeRequest request, RecomputeCallback callback) {
        final Recomputer recomputer = new Recomputer(request, callback);

        try {
            return recomputer.recompute();
        } catch (DegraphmalizerException e) {
            throw e;
        } catch (Exception e) {
            throw new WrappedException(e);
        }
    }
}
