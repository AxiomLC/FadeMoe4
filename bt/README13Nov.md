11 Nov 2025. Files are in bt\ folder of app.
#1 Overall structure. 3 scripts.
Top of script sketch of settings (subtle differences; brute no algoWindow, no tradeTS. tune no exchange expansion choices, only one exchange allowed per algo). This is the flow and area for User input; should be as close to top as possible. script top comments is always date/name/version, short description. Named sections where User could adjust error log levels. This rough sketch, not intended as perfect code:
                                            ========	
tune script:

TradeSettings->			
minPF:	1,			
tradeDir:	 'Long',    // Long, Short, Both
tradeSymbol:	{ useAll: true, list: ['ETH', 'BTC'] },
trade:	{ tradeWindow: 15, posVal: 1000, 
	tpPerc: [0.7,1.2,1.5], slPerc: [0.3,0.4,0.7] },
minTrades:	50,	maxTrades:	800,					
				
ComboAlgos->           
algo1:	MT; bin; rsi1_chg_5m; >; [15, 40],
algo2:	All; bin; [params]; >; [corePerc],
algo3:	''
				
AlgoSettings->			
algoWindow:	30,	// minutes algos can coincide
algoSymbol:	{ useAll: false, list:['ETH', 'BTC']},
corePerc:	[0.2,0.5,1.2,5,35,100]	
params:	[ 'c_chg_1m', 'c_chg_5m', 'c_chg_10m',
    'v_chg_1m', 'v_chg_5m', 'v_chg_10m',
    'oi_chg_1m', 'oi_chg_5m', 'oi_chg_10m',
    'pfr_chg_1m', 'pfr_chg_5m', 'pfr_chg_10m',
    'lsr_chg_1m', 'lsr_chg_5m', 'lsr_chg_10m',
    'rsi1_chg_1m', 'rsi1_chg_5m', 'rsi1_chg_10m',
    'rsi60_chg_1m', 'rsi60_chg_5m', 'rsi60_chg_10m',
    'tbv_chg_1m', 'tbv_chg_5m', 'tbv_chg_10m',
    'tsv_chg_1m', 'tsv_chg_5m', 'tsv_chg_10m',
    'lql_chg_1m', 'lql_chg_5m', 'lql_chg_10m',
    'lqs_chg_1m', 'lqs_chg_5m', 'lqs_chg_10m' ]
			
Output:				
topAlgos	15,			
listAlgos	30,			
  tradeTS:	FALSE			
				


=========================================================================================
brute	script:

BrTradeSettings:				
minPF:	1,			
tradeDir:	 'Long',    // Long, Short, Both
tradeSymbol:	{ useAll: true, list: ['ETH', 'BTC'] },
trade:	{ tradeWindow: 15, posVal: 1000, 
	tpPerc: [0.7,1.2,1.5], slPerc: [0.3,0.4,0.7] },
minTrades:	20,	maxTrades:	300,	
				
BrAlgoSettings:				
algoSymbol:	{ useAll: false, list: ['ETH', 'BTC'] },
exchange:	{ useAll: false, list: ['bin', 'byb, 'okx'] },
corePerc:	[0.2,0.5,1.2,5,35,100]	
params:	 [ 'c_chg_1m', 'c_chg_5m', 'c_chg_10m',
    'v_chg_1m', 'v_chg_5m', 'v_chg_10m',
    'oi_chg_1m', 'oi_chg_5m', 'oi_chg_10m',
    'pfr_chg_1m', 'pfr_chg_5m', 'pfr_chg_10m',
    'lsr_chg_1m', 'lsr_chg_5m', 'lsr_chg_10m',
    'rsi1_chg_1m', 'rsi1_chg_5m', 'rsi1_chg_10m',
    'rsi60_chg_1m', 'rsi60_chg_5m', 'rsi60_chg_10m',
    'tbv_chg_1m', 'tbv_chg_5m', 'tbv_chg_10m',
    'tsv_chg_1m', 'tsv_chg_5m', 'tsv_chg_10m',
    'lql_chg_1m', 'lql_chg_5m', 'lql_chg_10m',
    'lqs_chg_1m', 'lqs_chg_5m', 'lqs_chg_10m' ]
	
Output:				
topAlgos	15,			
listAlgos	50,			
				
=======================================================================================
algo-eng.js file = core functions, shared functions, any shared error and logging functions. It defines the parseAlgo language. It has global speed setting controls at top. It looks to consolidate shared functions so that tune and brute are not huge files. It exports core functions. File Naming:** JSON outputs will follow the `brute_MM-DD_HH-MM_utc.json` and `tune_MM-DD_HH-MM_utc.json` format.

brute.js file = brute force scan, test all of db to find high-PF standalone Algo1 signals across all symbols/exchanges. Has algo1 input params and trade settings at top. Algo1 does not need an algoWndow like tune. It can test all symbols, all exchanges, or just a few if User selects. It finds and sorts by PF the best pure single algos, e.g. All-short-byb_pfr_chg_5m>25. Outputs summary to console, full report to json file, and future usable endpoint export for future UI use. It exports topAlgos to console, and topAlgos, plus listAlgos to json (listAlgos is just the continuation after topAlgoes of a # of addtl results.) (brute\ folder).

tune.js file = comboAlgo tester. User input algo1 (generally static params) then AND algo2 (generally array of params to test). But any algo CAN have array of symbol, exchange, params, corePerc as input. *Important: comboAlgo testing uses algoSymbol only; not tradeSymbol. tradeSymbol only applied at simulateTrade. Multiple algos can be tested against eachother with variable inputs each, to find most profitable comboAlgo. Profitable comboAlgo are output/result with tradeSymbol and core trade settings attached for extra info. "OR" is deprecated. "AND" is hardcoded. Report to json file, and future usable endpoint export for future UI use. It exports topAlgos (comboAlgos) to console, and topAlgos, plus listAlgos to json (tune\ folder) (listAlgos is just the continuation after topAlgoes of a # of addtl results.) Option to output tradeTS json file. 

  * Each algo2 "combo" tested e.g. XRP_bin_v_chg_5m>15 creates its own bucket of ts, maybe tsA2C1 (combo1). That bucket then has to be scanned into algoWindow for hits; when true could become .trade etc. All individual .trade buckets have to be tested against simTrade/ simulate trade. here at simTrade apply the min/max trades, and the minPF thresholds. If pass - advances to pass.comboAlgo etc. Then all comboAlgos are sorted by PF and output/results etc

  * tune SEQUENCE: Algo1 combo1 batch scanned to perp_metrics = tsA1C1. Algo2 combo1 batch scanned to perp_metrics = tsA2C1. Algo2 combo2 batch scanned to perp_metrics = tsA2C2. Algo3 (if exists) etc. algoWindow applied to bucket tsA1C1 start (all these actions asc order in db, earliest to now); algoW.tsA1C1. tsA2C1 batch scanned into algowindow to find if true. true = ts.Trigger. ts.Trigger+1minute = tsA2C1.trade; trade entry ts (calc for "next ts" is safest; not 6000 milli calcs). Trade entry is then calc with tradeSymbol 'c' close(price) via bin exchange only. Trade simulation is ran with tradeWindow (minutes, start at tsA2C1.trade) applied and tp and sl. Close trade at tp or sl or tradeWindow end. 'Timeout' is a stat to be cached/ how many trades were closed due to tradeWindow end. 

  * seperate the ts buckets for each iteration of an algo. 
  * However choosing 'All' for algoSymbol allows for all symbols to be tested (sans MT) for the param and value, and all ts incl in ONE ts bucket.
  * If > then script tests all positive corePerc values; if < then script tests corePerc negative, e.g. -30 or -0.2

Algo1 should generally be static or only have a couple combos (but script remains flexible if it has more). It is known in db that the same ts can be repeated 3 times/ one each exchange. We want the 'unique' ts, no suplicates. But that doesnt change the fact that algo combo results in ONE bucket of unique ts; all algo2 combos result in ONE bucket each. 
Then scan/cascade algo2 combo1 tsA2C1 by algoW.tsA1C1 (window, or similar naming convention) to fetch any ts.Trigger. ts.Trigger+(nextTs)= tsA2C1.trade; to get ONE bucket of trades. A count is made of all tsA2C1.trade's and apply min/max trades. If over/under it is omitted from results; and info cached for summary output data. If pass min/max trades, simTrade engages. For speed - the script immediately batch fetch all 'c' close price for all tradeSymbol for tsA2C1.trade via ts_symbol_bin_c. So there will be a bucket of 'c' price (entry) for every tradeSymbol; e.g. BTC.tsA2C1.trade. simTrade is ran against tp sl and timeout, and PF secured. A User control at top can True/False allow threshold (50% default), that if PF not achieved by then script drops combo. If PF is passed, simTrade complete; other stats/ WR etc calc for results. 

               FLOWCHART: 
algo1 → tsA1C1 bucket (unique timestamps)
         ↓
algo2_combo1 (e.g. All;bin;v_chg_5m;>;15) → tsA2C1 bucket
algo2_combo2 (e.g. All;bin;v_chg_5m;>;25) → tsA2C2 bucket
algo2_combo3 (e.g. MT;bin;rsi_chg_10m;<;-30) → tsA2C3 bucket
         ↓
"Cascade" each combo's bucket (tsA2C1, tsA2C2, tsA2C3...) against tsA1C1 algoWindow
         ↓
        (inside algoW)
         tsA2C1.trigger
         tsA2C2.trigger
         tsA2C3.trigger
         ↓
  - Apply min/max trades filter
         ↓
         (passed min/ma)
         tsA2C1.trigger+'nextTs' = tsA2C1.trade
         tsA2C2.trigger+'nextTs' = tsA2C1.trade  
         ↓ 
  - fetch all symbol_bin_c price per ts .trade bucket
         ↓
simulateTrade() for EACH combo's .trade bucket:
  - cache tp, sl or timeout
  - calc PF
  - Apply minPF threshold
  - if 'Short' or 'Both' chosen as Dir, PnL calc is reversed and simTrade ran again for the Short.
  - If pass → comboAlgo passes ✅
         ↓
All passing comboAlgos sorted by PF → output/results
================================================================================================

#1A: Database: db is timescale hypertables postgres. perp_metrics table. with indexes: idx_metrics_filter ON perp_metrics (symbol, exchange, ts). it is 10 days, one minute floored ts. 'c' close (price) does exist for all symbols at each ts. Perp_metrics contains raw parms for each symbol: o,h,l,c,v, oi, pfr, lsr, rsi1, rsi60, tbv, tsv, lql, lqs. Followed by the chg_ params for each: 1m 5m 10m. the primary keys are ts, symbol, exchange. The ‘c’ close is price for trades. All trades are calc by symbol, bin (default exchange) and c.

#2 ParseAlgo language: the language is 5 sections separated by semi-colons. Functions in algo-eng and imported to scripts should  parse and know the format: "symbol; exchange; param; operator; corePerc". The language allows for various expansions; 'All' or a single entry 'byb', or an array [byb_v_chg_5m, okx_rsi1_chg_10m] [XRP, ETH, HYPE] [2.5, 24], or a placeholder name [params] [corePerc]. The format allows for ‘AND’ or + type function to test algos against eachother. (OR has been deprecated). The operator <> could be entered with both - where script has to run both, the positive and negative. 
e.g.        ETH; bin; pfr_chg_5m; >; 2.5            
e.g.        All; All; lsr_chg_10m; <>; [30,65]           
e.g.        [ETH,BTC,XRP]; [okx,byb]; rsi1_chg_1m; >; 15            
e.g.        ETH; bin; [params]; <>; 2.5          
e.g.        ETH; [bin,okx]; [lsr_chg_1m,tbv_chg_5m]; <; [corePerc] 

*note > tests positive corePerc values, and < tests negative values only. 
ComboAlgo: This expands to a comboAlgo language that can be used for export: tradeSymbol-dir-algoWindow-tradeWindow-posVal-tpPerc-slPerc-minTades-maxTrades->algo1+algo2. Or: All-Long-algoW:30-trW:15-tp:2-sl:0.5-min200-max-900-All;bin;pfr_chg_1m;>;25+BTC;bin;v_chg_10m;<;30 or something similar. The shortened comboAlgo is tradeSymbol-dir-algo1+algo2. e.g. for an input: All-Short-ETH; byb; pfr_chg_5m; >; 25 + All; [bin, okx]; [params]; <; [2,6.2,16]. Inputs allow arrays for testing multiple levels. 
Result comboAlgos have one item each - the most successful, e.g. All-Short-MT_bin_pfr_chg_5m>25 + All_okx_pfr_chg_5m<14.
The results - should be the "listAlgos" amount sorted by PF (the topAlgos is really just the top number of the full list). Outputs look like this:
ALL;Short;bin_rsi60_chg_10m<-1.5|TP2%|SL0.2%|Tr630|TO33%|WR43%|PF3.84
Or comboAlgo:  
All;Short;All_byb_rsi1_chg_5m<-14 + BTC_bin_lql_chg_1m>5.5|TP1.5%|SL0.5%|Tr455|TO55%|WR65%|PF1.24
Please: WR, Timeout (# of trades timed out without tp or sl) rounded to nearest whole. 
Tune script has an option to output the trade ts 'tradeTS' for future UI or AI evaluation (separate file also in tune\ folder): title “tuneTS:date/time" (like others).

#3 Addtl Info: 
A) MT token is to be excluded from tradeSymbol completely and 'All' list of algoSymbol. **But it can be a single manual entry as algoSymbol for an algo. Important to allow it as manual entry; and omit from algoSymbol All. 
B) all params generally have full history (some zeroes is ok. zeroes is not 'null'). Exceptions: bin/byb/okx: lql lqs; and okx/byb: tbv tsv; and okx/byb: rsi1 rsi60; are null naturally; no history. They do sometimes have data close to present time from websocket API scripts. Therefore, error catch should disregard these. 
C) Error Catch: should be simple one code lines at most major functions. But some can be yellow warnings. Only critical, script stopping errors (red) are written to db via dbmanager. If 50% of trade ts batch are tested and NO 'profitable'(no pass PF) trades found, yellow warning. If 30% of trade ts batch are tested and NO trades whatsoever, warning. if 40% of a chg_param is found null in the db, yellow warning (this warning or other similar errors have to have function exceptions, that it excludes: all: lql and lqs; and okx/byb: tbv and tsv; and okx/byb rsi1 and rsi60. These do not have full historic data avail in db. If 50% of db tested for Algo1 and no hits, warning; same for Algo2 etc. if 60% of profitable trades end in timeout, warning (def profitable: above Users PF settings). If User inputs for tune equals large amount of combinations to test, e.g. probably over 3 minutes at fastest settings - a warning issued at startup. This is a rough, fast calc. not perfect. If any of these warnings involve a calc that slows script – then re-organize so they do not. Normal findings at end of insufficient profitable or min or max trades violated, are normal log display, white, but can have red x emoji etc. These warning levels can be User controlled thresholds at top of script. An example of red error is 20%+ values are missing from ‘c’ close, for tradeSymbol. 
** LOGGING: #1 Startup console display - Starting Tune. Algo1: "MT; bin; rsi1_chg_5m; >; 20" + Algo2:  "All; bin; [params]; >; [corePerc]" Trade settings: minPF: | tradeDir: |tradeSymbols: All |trW: | minTrades: | maxTrades: | algoW:  #2 interim logs: "Algo1: 2 combos... fetched ts buckets, done. " "Algo2: 246 combos... fetched ts buckets, done."  "Cascading 598 comboAlgos in algoW..  200  passed!. 198 failed minTrades; 200 failed maxTrades." "simTrade 200 comboAlgos..." "190 failed minPF; 10 passed minPF!" "Results 1. All-Long-MT; bin_rsi1_chg_5m>20 + All; bin_lsr_chg_10m>5 |TP1.7%|SL0.3%|Tr 410|TO32%|Net$263|WR56%|PF1.3  ... etc
Results: C:\Users\q1fre\FadeMoe4\tune_output\tune_11-14_14-53_utc.json
Runtime: 0.45 min  *Do you see the output results format I would like?  current file screwed up and combined multiple tp/ sl combos.  Results should be singular param_value plus tp, sl winners. (Only symbols are allowed aggregate).

D) in tune, algo  input for 'exchange' is limited to one exchange entered in the algo, not All or []. 
E) Important : if Short or Both is chosen as direction, PnL calc is simply reversed to get the profit n loss from Short trades. If Both selected , its just double the work - but still the output is ranked by highest PF regardless of Direction. 

#4 Review for any better way to do this operation - using any tools. batch, pools, parallel, chunks, p-limit, etc. the local machine is robust at 16G ram big I5 processor and expanded postrgresql.conf file. #4 status logging and error catch should be streamlined. and interim heartbeat log should not slow process by extensive calc. Just simple 3 step process - 'Started' with the settings data and algo1-AND-algo2 info. then heartbeat log with basic summary of ops giving a basic idea of how fast the run might be. Then completion with topAlgos, addtl stats, and duration. **New, attached addtl file with spped test results for tune script type functions. 

=========================================================================================

=================================================================
AI chat NOTES

#1 Isnt "next ts" safer than +60000 milli? #2 the db is organized like this (ATTACHED). *Edit: tune, one exchange only allowed per algo entry. A User can enter multiple algos if they want to add testing ofr same param, different exchange. So code only need fetch one ts for symbol_exchange to put in bucket. multiple algoSymbol can still be entered and tested against, etc. #3 correct , algoWindow algoW starts at algo1 ts. #4 the parseAlgo language has to be flexible. Yes a User can use [params] and [corePerc] in the respective slots. [corePerc] means yes, of course, script is testing all the values defined in corePerc. Of course script will be slower, a User has to be diligent. 
*If > then script tests all positive corePerc values; if < then script tests corePerc negative, e.g. -30 or -0.2. Observer carefully the chg_ params in perp_metrics... they are % changes, some of them negative i.e. chg_5m -35; so the value of the param went down 35% in the last 5 min. if <> both is chosen by User, the script has to expand and test both ways. All of this is very expandable with unlimted algos added too; a User could setup a test and let it run 24 hours on his local - you understand? To keep our backtesting simple we look for an amount (%) of rise, or an amount of fall of the param. 
#5 Important: once a bucket of tsTrade or trade entry ts is secured, anything to do with algo testing/algoSymbol/params, values etc is DONE. never used again. the trade ts bucket is now used only by simulateTrades or "tradeSim" etc. and only Trade Settings is now used and tradeSymbol is used of course. Yes - it takes times, thats why we speed tested it. And still may need to tweak. If multiple tradeSymbol i.e. ['XRP','BTC','SOL] or All is used, the backtesting is looking for the most profitable comboAlgo that applies to all symbols as an aggregate. But each symbol still has to be tested (using bin exchange). If a User wants specific algos for specific symbols - he has to run more tests with just one tradeSymbol.

* 'All' used for tradeSymbol and algoSymbol: Results are for All (User input) tradeSymbols applied to a comboAlgo simTrade - it is aggregate right now. Aggregate PF/WR etc for all trade symbols applied. A User would have to run individual tests for single or a small related symbol group to get more detailed. 
Yes, in tune, if algoSymbol 'All' chosen, then for that combo algo ts bucket, All symbols (sans MT) are ran to gather the one singular ts .trigger bucket for that exchange_param_value. "Unique' is applied to ensure erase duplicates. if All chosen = AGGREGATE results; this applies to "Cascade" and to "SimTrade". 
#5 speed functions is controlled at algo-eng file.  #6 Are you not reading carefully.? the README iterated a whole section on logs and warnings... use common sense. the script runs and viewed in Console mostly, until UI developed. There would be warnings at key levels as described. there would be fail errors at fairly obvious major function steps. Use some judgement. But yes - would be nice for User to control the warning threshold levels at top; until we know better how these run. 

*NOW important -- if Long is chosen as Dir, the tradeSim looks for PnL as "long" trade/ buy-sell. if Short is chosen - tradeSIm reverses PnL calc, and trade entry is a "sell' (short), and exit a buy. But simply reversing PnL calc accomplishes it.

 Comments - nicely comment major function sections and single line comment on minor sections. tope of script date/title/ver and short descr of major functions. 

