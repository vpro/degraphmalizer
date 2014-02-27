package dgm.modules;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import dgm.modules.bindingannotations.Degraphmalizes;
import dgm.modules.bindingannotations.Fetches;
import dgm.modules.bindingannotations.Recomputes;
import dgm.modules.elasticsearch.DocumentProvider;
import dgm.modules.elasticsearch.QueryFunction;

import java.util.concurrent.*;

public class ThreadpoolModule extends AbstractModule
{
    private static final int MINTHREADPOOLSIZE = 4;
    private static final int MAXTHREADPOOLSIZE = 64;
    private static final int QUEUELIMIT = 65536;


    @Override
    protected final void configure()
    {
        bind(DocumentProvider.class).asEagerSingleton();
        bind(QueryFunction.class);
    }

    @Provides
    @Singleton
    @Degraphmalizes
    final ExecutorService provideDegraphmalizesExecutor()
    {
        // single threaded updates!
        final ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("degraphmalizer").build();

        return Executors.newSingleThreadExecutor(namedThreadFactory);
    }

    @Provides
    @Singleton
    @Recomputes
    final ExecutorService provideRecomputesExecutor()
    {
        final ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("recomputer-%d").build();

        return new ThreadPoolExecutor(MINTHREADPOOLSIZE, MAXTHREADPOOLSIZE,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(QUEUELIMIT),
                namedThreadFactory,new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Provides
    @Singleton
    @Fetches
    final ExecutorService provideFetchesExecutor()
    {
        final ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("fetcher-%d").build();

        return new ThreadPoolExecutor(MINTHREADPOOLSIZE, MAXTHREADPOOLSIZE,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(QUEUELIMIT),
                namedThreadFactory,new ThreadPoolExecutor.CallerRunsPolicy());
    }

}