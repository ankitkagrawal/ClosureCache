package groovy.com.closureCache

import java.text.DecimalFormat


Closure getDataClosure = {
    
    Thread.sleep(2*1000)
    
    return "Got it..."
    
}

def data

(1..10).each {

    long start = System.currentTimeMillis()
    data =  ClosureCache.instance.fetchData("data-id",30*1000,getDataClosure)
    long end = System.currentTimeMillis()

    println "Data - $data Took ${end - start} ms"

}

