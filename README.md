YahooPrices
===========
YahooPrices project refactored to run on AWS EC2 and store data to an RDS MySQL database.

This Python project takes advantage of the Yahoo server that streams "real-time" prices and other financial information to the finance.yahoo.com websites. A streaming connection to this server is established using python's requests package. Also, pertinent company information is scraped off of the Key Statistics pages for all of your stocks using BeautifulSoup4. Server and market timezones are accounted for using the pytz package.

All data that are captured are stored in a MySQL database hosted by Amazon RDS. 


Warning! This value of this information is non-existent in the context of making short term trading decisions. There are too many uncertainties as to when Yahoo decides to push price updates to the stream. Any use of this information to make investment decisions is entirely at your own risk.

##Why I did this

The primary goal of this project is to acumulate a source of fine grained price updates for historical and 'real-time' analysis. Hosting the MySQL database on AWS allows for good concurrency enabling storing and querying the database for analysis purposes.

##How to use it

Update the list of stock tickers to reflect your interests. Fill out the `dbparams.txt` file with your RDS information. Pull the repo to your EC2 server, run `streamerClient` with `nohup` so the script will run even with the terminal closed. Use  a cronjob to start the script just before the market opens. Don't forget that your server will be in a different timzone from New York.

This repo requires installation of the `pytz`, `beautifulsoup4` and `requests` packages.

##The Future

Current goals include:

1. Add multiprocessing with the goal of maintaining multiple streams from the yahoo server. More streams will increase the bandwidth for receiving updates.

2. Making eficiency improvements to the stringGen function which is called 2x for each byte that is parsed. While it works fine currently, I know I can do better. (Replaced `for` loop with list slicing realizing a 75% reduction in processing time.)

3. Improving the error handling around network errors so the client will continue in the case of an interuption.
