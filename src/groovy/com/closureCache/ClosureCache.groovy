package groovy.com.closureCache

import groovy.util.logging.Log4j

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Singleton
@Log4j
class ClosureCache {

    final Map<String,ClosureCacheItem> ivCache = new HashMap<String,Object>()

    long flushMillis =5*60*1000// 5 minutes.

    private ThreadPoolExecutor ivExec

    private ClosureCache()
    {
        ivExec=new ThreadPoolExecutor(
                10,    // Core pool size
                100,    // Max pool size
                5,    // Keep-alive time
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<Runnable>())
    }

    Object fetchData(String key, long refreshMillis,Closure dataAccessClosure )
    {
        fetchData(key, new ClosureCacheItemSettingsDTO(refreshMillis:refreshMillis),dataAccessClosure)
    }


    class RefreshWorker implements Runnable
    {

        private ClosureCacheItem ivItem;

        public RefreshWorker(ClosureCacheItem item)
        {
            ivItem=item
        }

        @Override
        void run() {
            try
            {
                if (log.isDebugEnabled())
                    log.debug("Running refresh on item")
                ivItem.refresh()
            }
            catch (Throwable th)
            {
                log.error("Failure during item refresh",th)
            }
        }
    }

    private void ensureRefreshQueued(ClosureCacheItem item)
    {
        if (!item.lastRefreshWorker)
        {
            synchronized(item)
            {
                if(!item.lastRefreshWorker)
                {
                    //Instead of refreshing directly, execute a worker to refresh so we are not waiting and blocking everyone waiting for this item.
                    if(log.isDebugEnabled())
                        log.debug("Queuing refresh:"+ item.key)
                    RefreshWorker rw = new RefreshWorker(item)
                    ivExec.execute(rw)
                    item.lastRefreshWorker=System.currentTimeMillis()
                }

            }
        }
        else
        {
            //check that its been refreshed after at least 10 seconds
            Long lastRefreshWorker =item.lastRefreshWorker
            if (lastRefreshWorker && System.currentTimeMillis()-lastRefreshWorker>30000)
            {
                log.error("Failed to refresh item after 10 seconds:"+ item.key + " last updated (millis ago):${System.currentTimeMillis()-item.lastUpdated}, resetting so it gets updated." )
                item.lastRefreshWorker=null
            }
        }
    }

    Object fetchData(String key, ClosureCacheItemSettingsDTO settings,Closure dataAccessClosure )
    {
        long now = System.currentTimeMillis()

        ClosureCacheItem item =null;
        synchronized (ivCache)
        {
            item=ivCache.get(key)
            if(!item)
            {
                item= new ClosureCacheItem(key:key, dataAccessClosure:dataAccessClosure, refreshPeriodMillis:settings.refreshMillis, autoRefresh:settings.autoRefresh)
                ivCache.put(key,item)
            }
        }

        if (item.lastUpdated==0)//its new and never been updated.
        {
            ensureRefreshQueued(item)

            //Wait for item to be refreshed before returning
            item.waitForRefresh()
        }


        long age=now-item.lastUpdated
        boolean stale = age>(item.refreshPeriodMillis)

        if(log.isDebugEnabled())
            log.debug("key:"+key+" stale:"+ stale + " refresh:"+ item.refreshPeriodMillis + " age(last update ago):"+ age)
        if(stale)
        {
            ensureRefreshQueued(item)
        }
        item.lastAccess=now

        return item.value

    }

    void refresh()
    {
        Map<String,ClosureCacheItem> copy

        synchronized(ivCache)
        {
            copy=new HashMap<String,ClosureCacheItem>(ivCache)
        }


        List<String> keyCopy = new ArrayList<String>(copy.keySet())
        keyCopy.each({String key->

            ClosureCacheItem item = copy.get(key)
            long now =System.currentTimeMillis()
            if (item.autoRefresh && (item.value==null || now-item.lastUpdated>(item.refreshPeriodMillis*0.75)))
            {
                ensureRefreshQueued(item)
            }

            if (now-item.lastAccess>flushMillis)
            {
                synchronized (ivCache)
                {
                    ivCache.remove(key)
                }
            }

        })

    }
}

