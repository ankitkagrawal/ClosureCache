package groovy.com.closureCache

import groovy.util.logging.Log4j

/**
 * Created by ankit on 17/6/15.
 */
@Log4j
class ClosureCacheItem {

    String key
    Closure dataAccessClosure
    Object value
    long lastUpdated
    long refreshPeriodMillis
    long lastAccess

    //Uses closure job to refresh this item when 75% of the refresh time has elapsed.
    boolean autoRefresh=true
    Long lastRefreshWorker



    void refresh()
    {
        try
        {
            if (log.isDebugEnabled())
                log.debug("Refreshing item:"+ key)

            value=dataAccessClosure()
            lastUpdated=System.currentTimeMillis()
            lastRefreshWorker=null



        }
        catch (Throwable e)
        {
            log.error("Failed to refresh closure cache item",e)
        }
        finally
        {
            synchronized (this)
            {
                notifyAll()
                //Even if we have an error, can't let everyone else be blocked forever.
            }

        }
    }

    void waitForRefresh()
    {
        synchronized (this)
        {
            wait(600000) //wait 10 minutes at most
        }
    }
}
