
# Generic API Storage Plugin
This plugin is intended to enable you to query APIs over HTTP/REST.  At this point, the API reader will only accept JSON as input however in the future, it may be possible to
 add additional format readers to allow for APIs which return XML, CSV or other formats.  
 
Note:  This plugin should **NOT** be used for interacting with tools which have REST APIs such as Splunk or Solr.  It will not be performant for those use cases.  

## Configuration
To configure the plugin, create a new storage plugin, and add the following configuration options:

```
{
  "type": "http",
  "connection": "https://api.sunrise-sunset.org/",
  "resultKey": "results",
  "enabled": true
}
```
The options are:
* `type`:  This should be `http`
* `connection`:  This should be the root level URL for your API.
* `resultKey`:  The result key is the key in the results which contains a table-like structure of the data you want to retrieve.  Using this will help you eliminate other data
 that often is transmitted in API calls. 
 



samples:

    select sunrise, sunset from api.`/json?lat=36.7201600&lng=-4.4203400&date=today`;
 
    
not support (still working on):

    select name from (select name, length from http.`/e/api:search` where $q='avi' and $p=2) where length > 0
