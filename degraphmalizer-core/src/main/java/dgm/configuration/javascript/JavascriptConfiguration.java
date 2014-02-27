package dgm.configuration.javascript;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.tinkerpop.blueprints.Direction;
import dgm.JSONUtilities;
import dgm.Subgraph;
import dgm.configuration.*;
import dgm.exceptions.ConfigurationException;
import dgm.exceptions.ValueIsAbsentException;
import dgm.graphs.Subgraphs;
import dgm.modules.elasticsearch.ResolvedPathElement;
import dgm.trees.Tree;
import dgm.trees.Trees;
import org.mozilla.javascript.*;
import org.mozilla.javascript.tools.shell.Global;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Load configuration from javascript files in a directory
 */
public class JavascriptConfiguration implements Configuration {

    private static final Logger LOG = LoggerFactory.getLogger(JavascriptConfiguration.class);

    private final Map<String, JavascriptIndexConfig> indices = new HashMap<String, JavascriptIndexConfig>();

    //private JavascriptFixtureConfiguration fixtureConfig;

    static {
        ContextFactory.initGlobal(new JavascriptContextFactory());
    }

    public JavascriptConfiguration(ObjectMapper om, String directory, URL... libraries) throws IOException {
        LOG.info("Reading {} with libraries {}", directory, Arrays.asList(libraries));
        final List<String> directories = Configurations.listDirectories(directory);
        if (directories == null) {
            throw new ConfigurationException("Configuration directory " + directory + " does not exist");
        }
        for (String dir : directories) {
            // each subdirectory encodes an index
            String[] dirArray = dir.split("/");
            String dirname = dirArray[dirArray.length -1];
            LOG.info("Reading {} ({})", dir, dirname);

            indices.put(dirname, new JavascriptIndexConfig(om, dirname, dir, libraries));
        }
    }


    @Override
    public Map<String, ? extends IndexConfig> indices() {
        return indices;
    }

    private static class JavascriptContextFactory extends ContextFactory {
        @Override
        public boolean hasFeature(Context context, int featureIndex) {
            switch (featureIndex) {
                case Context.FEATURE_STRICT_MODE:
                    return true;
                case Context.FEATURE_DYNAMIC_SCOPE:
                    return true;
            }
            return super.hasFeature(context, featureIndex);
        }
    }


}


class JavascriptIndexConfig implements IndexConfig {
    private static final Logger LOG = LoggerFactory.getLogger(JavascriptIndexConfig.class);

    final String index;
    final Scriptable scope;
    final Map<String, JavascriptTypeConfig> types = new HashMap<String, JavascriptTypeConfig>();


    /**
     * Scan, filter and watch a directory for correct javascript files.
     *
     * @param index     The elastic search index to write to
     * @param directory Directory to watch for files
     */
    public JavascriptIndexConfig(ObjectMapper om, String index, String directory, URL... libraries) throws IOException {

        LOG.info("ES: {}, directory: {}, libraries {}", new Object[]{index, directory, Arrays.asList(libraries)});
        this.index = index;
        ScriptableObject buildScope;

        try {
            final Context cx = Context.enter();

            // create standard ECMA scope (org.mozilla.javascript.ImporterTopLevel) including some rhino utilities from Global
            buildScope = new Global(cx); //cx.initStandardObjects(null, true);

            loadLibs(cx, buildScope, libraries);

            final Object jsLogger = Context.javaToJS(new JSLogger(), buildScope);
            ScriptableObject.putProperty(buildScope, "LOG", jsLogger);
            // seal the libraries and the loggers so configurations can't overwrite them.
            buildScope.sealObject();

            // non recursively load all configuration files
            final Predicate<URL> filenameFilter = new Predicate<URL>() {
                @Override
                public boolean apply(URL url) {
                    return url.getFile().endsWith(".conf.js");

                }
            };

            final List<URL> configFiles = Configurations.list(directory, filenameFilter);
            if (configFiles == null) {
                throw new ConfigurationException("Configuration directory " + directory + " can not be read");
            }
            LOG.info("{}: Found config files  for index [{}]", directory, configFiles);
            for (URL file : configFiles) {
                final Reader reader = new InputStreamReader(file.openStream(), "UTF-8");
                final String fn = file.toString();
                final String type = file.getFile().replaceFirst(".conf.js", "").replaceFirst(".*/", "");

                LOG.info("Found config file [{}] for index [{}] and type [{}]", new Object[] {file, index, type});

                final Scriptable typeConfig = (Scriptable) compile(cx, buildScope, reader, fn);

                types.put(type, new JavascriptTypeConfig(om, type, buildScope, typeConfig, this));
            }

            // Seal the configuration.
            buildScope.sealObject();
        } finally {
            Context.exit();
        }

        this.scope = buildScope;
    }


    @Override
    public String name() {
        return index;
    }

    @Override
    public Map<String, ? extends TypeConfig> types() {
        return types;
    }

    private Object compile(Context cx, Scriptable scope, Reader reader, String fn) throws IOException {
        // compile and execute into the scope
        return cx.compileReader(reader, fn, 0, null).exec(cx, scope);
    }


    private void loadLibs(Context cx, Scriptable scope, URL... urls) throws IOException {
        for (URL f : urls) {
            loadLib(cx, scope, f);
        }
    }

    private Object loadLib(Context cx, Scriptable scope, URL f) throws IOException {
        final Reader reader = new InputStreamReader(f.openStream(), "UTF-8");
        Object object = compile(cx, scope, reader, f.getFile());
        LOG.info("Loaded {}", f);
        return object;
    }

    @Override
    public String toString() {
        return "JavascriptIndexConfig(index=" + index + ")";
    }
}

class JavascriptTypeConfig implements TypeConfig {
    private static final Logger LOG = LoggerFactory.getLogger(JavascriptTypeConfig.class);
    final IndexConfig indexConfig;
    final String type;
    final Scriptable scope;
    final Scriptable script;

    final Function filter;
    final Function extract;
    final Function transform;

    final String sourceIndex;
    final String sourceType;

    final ObjectMapper objectMapper;


    final Map<String, WalkConfig> walks = new HashMap<String, WalkConfig>();

    public JavascriptTypeConfig(ObjectMapper objectMapper, String type, Scriptable scope, Scriptable script, IndexConfig indexConfig) throws IOException {
        this.objectMapper = objectMapper;
        this.type = type;
        this.scope = scope;
        this.script = script;
        this.indexConfig = indexConfig;

        LOG.debug("Creating config for type [{}] in index [{}]", type, indexConfig.name());

        try {
            Context.enter();

            // filter & graph extraction functions
            filter = (Function) fetchObjectOrNull("filter");
            extract = (Function) fetchObjectOrNull("extract");
            transform = (Function) fetchObjectOrNull("transform");

            //TODO: null check, invalid configuration, error handling
            sourceIndex = ScriptableObject.getTypedProperty(script, "sourceIndex", String.class);
            sourceType = ScriptableObject.getTypedProperty(script, "sourceType", String.class);


            // add the walks
            final Scriptable walks = (Scriptable) fetchObjectOrNull("walks");
            if (walks != null) {
                for (Object id : ScriptableObject.getPropertyIds(walks)) {
                    final String walkName = id.toString();

                    // get the walk object
                    final Scriptable walk = (Scriptable) ScriptableObject.getProperty(walks, walkName);

                    final Direction direction = Direction.valueOf(ScriptableObject.getProperty(walk, "direction").toString());

                    Integer  maxDistance = ScriptableObject.getTypedProperty(walk, "maxDistance", Integer.class);
                    if (maxDistance == null) maxDistance = Integer.MAX_VALUE;


                    final Scriptable properties = (Scriptable) ScriptableObject.getProperty(walk, "properties");

                    final JavascriptWalkConfig walkCfg = new JavascriptWalkConfig(objectMapper, walkName, direction, maxDistance, this, scope, properties);

                    this.walks.put(walkName, walkCfg);
                }
            } else {
                LOG.debug("No walks found in configuration");
            }
        } finally {

            Context.exit();
        }
    }

    private Object fetchObjectOrNull(String field) {
        final Object obj = ScriptableObject.getProperty(script, field);

        // field not specified in script
        if (obj == UniqueTag.NOT_FOUND)
            return null;

        return obj;
    }


    @Override
    public String name() {
        return type;
    }

    @Override
    public Subgraph extract(JsonNode document) {
        if (document == null) {
            throw new NullPointerException("Must pass in non-null value to extract(..)");
        }

        if (extract == null) {
            LOG.debug("Not extracting subgraph because no extract() function is configured");
            return Subgraphs.EMPTY_SUBGRAPH;
        }

        JavascriptSubgraphImpl sg;
        try {
            final Context cx = Context.enter();
            final Scriptable threadScope = cx.newObject(scope);
            threadScope.setPrototype(scope);
            threadScope.setParentScope(null);

            // extract graph components
            sg = new JavascriptSubgraphImpl(objectMapper, cx, threadScope);

            final Object obj = JSONUtilities.toJSONObject(cx, threadScope, document);
            extract.call(cx, threadScope, threadScope, new Object[]{obj, sg});
        } finally {
            Context.exit();
        }
        return sg.subgraph;
    }

    @Override
    public boolean filter(JsonNode document) {
        if (filter == null) {
            return true;
        }

        boolean result = false;

        try {
            final Context cx = Context.enter();
            final Scriptable threadScope = cx.newObject(scope);
            threadScope.setPrototype(scope);
            threadScope.setParentScope(null);
            final Object doc = JSONUtilities.toJSONObject(cx, threadScope, document);
            result = Context.toBoolean(filter.call(cx, threadScope, threadScope, new Object[]{doc}));
        } finally {
            Context.exit();
        }

        return result;
    }

    @Override
    public JsonNode transform(JsonNode document) {

        if (transform == null) {
            LOG.trace("No transformation function is configured, processing document as-is.");
            return document;
        }

        try {
            final Context cx = Context.enter();
            final Scriptable threadScope = cx.newObject(scope);
            threadScope.setPrototype(scope);
            threadScope.setParentScope(null);
            final Object doc = JSONUtilities.toJSONObject(cx, threadScope, document);
            final Object result = transform.call(cx, threadScope, threadScope, new Object[]{doc});
            return JSONUtilities.fromJSONObject(objectMapper, cx, threadScope, result);
        } catch (IOException e) {
            //TODO: and what about error handling???
            throw new RuntimeException("Could not transform the input document.", e);
        } finally {
            Context.exit();
        }
    }

    @Override
    public IndexConfig index() {
        return indexConfig;
    }

    @Override
    public String targetType() {
        return name();
    }

    @Override
    public String sourceIndex() {
        return sourceIndex;
    }

    @Override
    public String sourceType() {
        return sourceType;
    }

    @Override
    public String targetIndex() {
        return index().name();
    }

    @Override
    public Map<String, WalkConfig> walks() {
        return walks;
    }

    @Override
    public Integer maximalWalkDepth() {
        int result = 0;
        for (WalkConfig walkConfig : walks().values()) {
            if (walkConfig.maxDistance() > result) {
                result = walkConfig.maxDistance();
            }
        }
        return result;
    }


    @Override
    public String toString() {
        return getClass().getName() + ' ' + sourceIndex() + '/' + sourceType() + "->" + targetIndex() + '/' + targetType() + "(" + maximalWalkDepth() + ")";
    }
}

class JavascriptWalkConfig implements WalkConfig {
    final String walkName;
    final Direction direction;
    final TypeConfig typeCfg;

    final Integer maxDistance;

    // TODO use guava immutables
    final Map<String, JavascriptPropertyConfig> properties = new HashMap<String, JavascriptPropertyConfig>();


    public JavascriptWalkConfig(ObjectMapper om, String walkName, Direction direction, Integer maxDistance, TypeConfig typeCfg, Scriptable scope, Scriptable propertyScriptable) {
        this.walkName = walkName;
        this.direction = direction;
        this.maxDistance = maxDistance;
        this.typeCfg = typeCfg;


        try {
            Context.enter();

            // add all the properties
            for (Object id : ScriptableObject.getPropertyIds(propertyScriptable)) {
                final String propertyName = id.toString();
                final Scriptable property = (Scriptable) ScriptableObject.getProperty(propertyScriptable, propertyName);

                final Function reduce = (Function) ScriptableObject.getProperty(property, "reduce");
                final boolean nested = ScriptableObject.getProperty(property, "nested").toString().equals("true");

                this.properties.put(propertyName, new JavascriptPropertyConfig(om, propertyName, nested, reduce, scope, this));
            }
        } finally {
            Context.exit();
        }
    }

    @Override
    public Direction direction() {
        return direction;
    }

    @Override
    public TypeConfig type() {
        return typeCfg;
    }

    @Override
    public Map<String, ? extends PropertyConfig> properties() {
        return properties;
    }

    @Override
    public String name() {
        return walkName;
    }

    @Override
    public Integer maxDistance() {
        return maxDistance;
    }
}


class JavascriptPropertyConfig implements PropertyConfig {
    final String name;
    final boolean nested;
    final Function reduce;
    final Scriptable scope;
    final WalkConfig walkConfig;
    final ObjectMapper om;

    private static final Logger LOG = LoggerFactory.getLogger(JavascriptPropertyConfig.class);


    public JavascriptPropertyConfig(ObjectMapper om, String name, boolean nested, Function reduce, Scriptable scope, WalkConfig walkConfig) {
        this.om = om;
        this.nested = nested;
        this.name = name;
        this.reduce = reduce;
        this.scope = scope;
        this.walkConfig = walkConfig;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public JsonNode reduce(Tree<ResolvedPathElement> tree) {
        JsonNode result = null;

        try {
            final Context cx = Context.enter();
            final Scriptable threadScope = cx.newObject(scope);
            threadScope.setPrototype(scope);
            threadScope.setParentScope(null);

            final com.google.common.base.Function<ResolvedPathElement, JavascriptNode> elementToNode = new com.google.common.base.Function<ResolvedPathElement, JavascriptNode>() {
                @Override
                public JavascriptNode apply(ResolvedPathElement input) {
                    return new JavascriptNode(threadScope, input);
                }
            };
            final Tree<JavascriptNode> javascriptTree = Trees.map(elementToNode, tree);
            final Object reduceResult = reduce.call(cx, threadScope, null, new Object[]{javascriptTree});

            result = JSONUtilities.fromJSONObject(om, cx, threadScope, reduceResult);
        } catch (JsonProcessingException e) {
            LOG.error(e.getMessage(), e);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        } catch (WrappedException e) {
            // To make catching incomplete trees work.
            Throwable throwable = e.getWrappedException();
            if (throwable instanceof ValueIsAbsentException) {
                throw (ValueIsAbsentException)throwable;
            }
            throw e;
        } finally {
            Context.exit();
        }

        return result;
    }

    @Override
    public WalkConfig walk() {
        return walkConfig;
    }
}
