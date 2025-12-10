27 Nov - SUMMARY -
Related to fade.moe UI front end.
db\dbsetup is the structure of postgresql timescale hypertables database with indexes: ts.symbol.exchange of perpetual futures crypto symbol data fetched from Binance, Bybit, Okx. Inserted into the 1 min interval 10days rolling/ pruning db.
The params for the crypto symbols (perp_data table) are symbol, exchange, ohlcv, oi, pfr, lsr, rsi1, rsi60, tbv, tsv, lql, lqs. lql and lqs are sparsely populated and rsi params are internally calc.
MT token is an internal created index token. 
Master-api.js begins the fetch process (pulls symbols from perp-list.js in root) - with backfill first, followed by real-time continuous pull. That file triggers calc-metrics.js file, which trigger backfill-metriccs.js first to backfill perp_metrics table, and then they perfom real-time calcs for perp_metrics table which is the crucial _chg_ params for algo backtesting. The api-utils.js file assists api scripts with shared functions. 
The scripts in apis\ folder perfom the backfill and real-time pull from the exchanges via api and websocket. 
The server serves the front end which is public\ folder files. 
the bt\ folder contains brute.js and tune3.js which manually perfrom backtesting the params in perp_metrics and output to console and json files. These are run CLI and not integrated to UI/UX yet. The readme-s\ files explain that further.
In public\ besides the dbview, ther is the Alert Cards view, cardsview.html. This UI is set so a User could manually enter comboAlgos found through brute ans tune runs and have live Alerts polled and displayed at 1 minute intervals. This would assist a User manually trading on any of the exchanges. the files in public\js\ are of course crucial js function scripts for all the aspects of the frint end UI.
