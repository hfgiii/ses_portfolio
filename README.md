## ses_portfolio

This project exists to exercise [elasticsearch](http://elasticsearch.org) and with scala elasticsearch client, [elastic4s](https://github.com/sksamuel/elastic4s). The examples in this project are inspired by problem sets in the [Coursera](https://www.coursera.org/), [Computational Investing, Part I](https://www.coursera.org/course/compinvesting1).

In particular, current examples use elaticsearch to :

* Create portfolios with __N__ equities and benchmark indexes as elasticsearch [indexes](http://www.elasticsearch.org/blog/what-is-an-elasticsearch-index/) using historical price data from [Yahoo Finance](http://finance.yahoo.com/), downloaded as CSV files. The lines in the CSV files are translated into JSON using [CSVParboiledParser by Juergen Pfundt](http://poundblog.wordpress.com/2014/08/23/a-scala-parboiled2-grammar-for-csv/). The portfolios created are:

  1. Simple elasticseatch index containing the following document with daily prices for each equity, e.g.:
  
     ```
     { "name": "msft,     
        "date": "2011-01-3",
        "open": 30.25,
        "high": 30.52,
        "low" : 30.15,
        "close" : 30.28,
        "volume": 1234567,
        "adj_close: 30.26 
      }
      ```
      
    This index is used to calculate sharpe ratio examples explained in the following bullets.
    
  2. A simple elastic search index that aggregates the daily adjusted closing price (adj_close) for four equities into one document,e.g.:
  
     ```
     { "date" : "2011-01-03",
        "msft_closing" : 30.26,
        "amzn_closing" : 184.22,
        "ebay_closing" : 28.68,
        "ups_closing"  : 65.48
      }
      ```
        
   This index is used to maximize the sharpe ratio of a four equity portfolio. 
 
* Calculate the [sharpe ratio](http://en.wikipedia.org/wiki/Sharpe_ratio) for individual equities using elasticsearch [extended stats aggregation](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations-metrics-extendedstats-aggregation.html);
* Calculate the sharpe ratio for multiple equities using elasticsearch [terms aggregation](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html) to create one bucket per equity and the extended stats aggregation in each bucket.
* Calculate the weights of allocation for a four equity portfolio that maximize the sharp ratio for the portfolio as a whole. This example uses and elasticsearch [script field](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-request-script-fields.html), to distribute the allocation amongst the closing prices. 

### How to Run Examples

From the commandline run the following sbt command: 
   ```
     sbt "project analytics" "run <args>"     
   ``` 
   
_The initial response from sbt will be:_
  
_Multiple main classes detected, select one to run:_

 _[1] org.hfgiii.sesportfolio.analytics.AnalyticsMain_
 _[2] org.hfgiii.sesportfolio.analytics.client.esexamples.EsExamples_

_Enter number:_

If you enter _2_, there is no need for arguments and `<args>` is empty. The first selection, _org.hfgiii.sesportfolio.analytics.AnalyticsMain_ requires arguments. The following table shows the valid input:

| Option   | ValidValues | Calculation |
| :------: | :---------: | :---------: |
|   o      | true,false  | optimize portfolio |
|   s      | true,false  | calc sharpe ratio |
|  e       | msft,amzn,ebay,ups| combine with _s_ for single sharpe ratio calc|

Notice: If one enters the _s_ option without a an _e_ option, the sharpe ratio is calculated for four equities: _msft, amzn, ebay, ups_. The _e_ option by itself with a valid equityis a __NOOP__; the example will exit silently. If the equity is not valid, it responds with following message:" '__invalid equity name__' is not a valid equity for sharpe ratio calculation"

Issue: when sbt runs the examples it will throw a stack trace most of the times when the example code shutdowns elasticsearch. Will fix this shortly. 
