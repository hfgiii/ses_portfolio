## ses_portfolio

This project exists to exercise [elasticsearch](http://elasticsearch.org) with scala elasticsearch client, [elastic4s](https://github.com/sksamuel/elastic4s). The examples in this project are inspired by problem sets in the [Coursera](https://www.coursera.org/) course, [Computational Investing, Part I](https://www.coursera.org/course/compinvesting1).

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
      
    This index is used to calculate sharpe ratio examples as explained in the following bullets.
    
  2. A simple elastic search index whose documents contain the daily rate of return (ex dividend) for four equities,e.g.:
  
     ```
     { "date" : "2011-01-04",
        "msft_ror" : 0.001345,
        "amzn_ror" : 0.004589,
        "ebay_ror" : 0.0145,
        "ups_ror"  : 0.00111
      }
      ```
   This index is used to maximize the sharpe ratio of a four equity portfolio. 
   
  3. A simple elastic search index whose documents contain the weekly rate of return (ex dividend) for Micsrosoft (MSFT) and S&P 500 index equities,e.g.:
  
     ```
     {  "date" : "2011-01-14",
        "msft_ror" : 0.001745,
        "snp_ror"  : 0.003547
      }
      ```
   This index is used to calclulate for MSFT the beta coefficient of the Capital Asset Pricing Model (CAPM).
 
* Calculate the [sharpe ratio](http://en.wikipedia.org/wiki/Sharpe_ratio) for individual equities using the elasticsearch [extended stats aggregation](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations-metrics-extendedstats-aggregation.html);
* Calculate the sharpe ratio for multiple equities using elasticsearch [terms aggregation](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html) to create one bucket per equity and then using extended stats aggregation in each bucket to calculate the sharpe ratio for each equity.
* Calculate the weights of allocation for a four equity portfolio that maximize the sharp ratio for the portfolio as a whole. This example uses the elasticsearch [script field](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-request-script-fields.html), to distribute the allocation weights amongst the closing prices. 
* Calculate [beta CAPM coefficient](http://en.wikipedia.org/wiki/Capital_asset_pricing_model)for MSFT against the S&P 500 over a 3 year peropd from 2011 to 2014. The calculation uses a linear regression algorithm implemented in [scalanlp](http://www.scalanlp.org), in particular, with the [breeze](https://github.com/scalanlp/breeze) and [nak](https://github.com/scalanlp/nak) components.

### How to Run Examples

From the commandline run the following sbt command: 
   ```
     sbt "project analytics" "run <args>"     
   ``` 
   
The initial response from sbt will be:
  
_Multiple main classes detected, select one to run:_

 _[1] org.hfgiii.sesportfolio.analytics.AnalyticsMain_
 
 _[2] org.hfgiii.sesportfolio.analytics.client.esexamples.EsExamples_

_Enter number:_

If one enters _2_, there is no need for arguments and `<args>` is empty. The first selection, _org.hfgiii.sesportfolio.analytics.AnalyticsMain_, requires arguments. The following table shows the valid input:

| Option   | ValidValues | Calculation |
| :------: | :---------: | :---------: |
|   o      | true,false  | optimize portfolio |
|   s      | true,false  | calc sharpe ratio |
|  e       | msft,amzn,ebay,ups| combine with _s_ for single sharpe ratio calc|
|  b       | true        | calc CAPM beta for MSFT|

Notice: If one enters the _s_ option without an _e_ option, the sharpe ratio is calculated for four equities: _msft, amzn, ebay, ups_. The _e_ option by itself with a valid equity is a __NOOP__; the example will exit silently. If the equity is not valid, it responds with following message:" '__invalid equity name__' is not a valid equity for sharpe ratio calculation"

Issue: when sbt runs the examples, it will throw a stack trace most of the times when the example code shutdowns elasticsearch. Will fix this shortly. 
