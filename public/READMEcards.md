Review: create Alert cards to be displayed in the "Alert Cards: div in the UI front end; this can be understood by the cardsview.html placeholder file.
Functions would be another file (card-poll.js), other AI suggested faster if in backend, db\ folder. And some AI said front end is fine. So - it would be a file: "card-poll.js" that parses out comboAlgos, polls live the perp_metrics table and creates dynamic scrolling Alerts for trades for the symbols that issue a trigger per the "comboAlgo" entered by a User..

Alert Cards UI:
cardsview.html file uses styles from style.css.
#1 Start/Stop control buttons at top, for live polling. 
Review button functions: Start button is to start polling live, with live green glow when active and a small status text "live" appears next to button if enabledd; and Stop button stops polling and alerts and button glows red then, and small text to right of button: "stopped" if enabled. Default is Stopped.
#2 Have another User control box (comboAlgo User entry box) next to Start/Stop controls that says "1st Alert Delay". Allow User to input a number 1-100, that the code functions read as minutes. And these minutes are a DELAY from the 1st time that 'symbol_alert' triggers, until it is eligible to trigger again. 
#3 Lets have another User control box next to 1st Alert Delay called: "max Display Alerts". Allow User to input a number 1-100, that the code functions read as max_alerts displayed. They are self-pruned and deleted, uneeded; unless we change this later.
#3A Add "Wipe" button next to Alert Delay as mentioned below. 

#4 There would be 1 empty box ('input.box', comboAlgo User entry box) by default below the control btns, with title "Enter comboAlgo:" above it. with a + sign to add more boxes if User wants to monitor more Alerts. System could handle 1-10 Alerts. - sign button to delete unused boxes below the first. Lets only have one input box as default , with the + to add more, and a - to subtract any below the first. 
* Add a "#1" to the first input_box; if User adds more, second becomes #2 etc. THen - on Alert Cards put a #1 next to comboAlgo, i.e. "#1 ETH,BTC,XRP;Long;MT_bin_rsi1_chg_1m>30 + MT_bin_v_chg_5m>20" (the #1 can be normal font, the comboAlgo still small); if that Alart was generated from comboAlgo1. get it?
* comboAlgo User entry box: uses th parseAlgo language; that it can parse from entries like e.g.: All;Short;MT_bin_rsi1_chg_1m<30 + MT_bin_v_chg_5m>20 + BTC_bin_oi_chg_10m<0.2  or  All;Short;MT_bin_rsi1_chg_1m<30 + MT_bin_v_chg_5m>20  or  ETH,BTC,XRP;Long;MT_bin_rsi1_chg_1m>30 + MT_bin_v_chg_5m>20 + BTC_bin_oi_chg_10m>0.4

#5 Generated Alert Cards: Flex grid displays below control buttons. Flex grid would default 4 across and 20 down (older would go out of view, prune). But it is flex, so smaller screen or zooming will auto-adjust. 
Alert Card - square box each, would display: Time(small font, human readable utc), Direction(big font, green or red), Symbol, and the comboAlgo(smaller font). (later, incorporate static simple 1min crypto line price chart). Shorts would have title Short in red font, Longs, in green font.

* Grid dynamically updating anytime new Alert triggered. new Alert on top. But script is monitoring every 1 minute perp_metrics table. But Alerts only display when triggered. Obviously use same type styles index uses now.
 
* Be careful that when "Alert Cards" button pressed, the Alert Cards div does not include all the control buttons that are part of dbview/ db controls.
## ==============================================================================

Notes:
parseAlgo language as used in apps other files: Yes symbols must be caps. the comboAlgo is a copy/paste from console results output from another app file. User just copy/paste. * Only + used between algos to form comboAlgo. # < or >, per each param. < or > is just a simple threshold, higher than or lower than. 
Algo language is: tradeSymbol(s);direction;algoSymbol_exchange_param_operator_value. and + to connect addtl algos for a comboAlgo. 
tradeSymbol and algoSymbol can be e.g.: XRP; or XRP,BTC,DOGE; or All;. The rest are single item entry only. 'All' omits MT, but MT can be single entry in algoSymbol. 

* db perp_metrics table functions at 1 min intervals. Alerts would be continuous 1 min polling for all alerts.
* the Alerts code in card-poll.js has to be scanning for EACH symbol (algosymbol_exchange). It issues an Alert PER symbol only. It is true that Alerts might flood if all symbols have a similar alert, but we need to test to see how this works. *important: pareseAlgo language code must omit MT symbol from 'All' algoSymbol input, and 'All' tradeSymbol polling for Alerts. (it is an app created  index symbol, not traded). 

* Re-review dbsetup and server files. perp_metrics is the ONLY table we are dealing with. It has raw ts and symbol and exchange columns as well as all the chg_ columns that are used. Eval: how best to live_poll this table?? code new API into server file? Consider there could be 30-40 symbols, and 3 or 4 comboAlgos (max); thats a lot of polling traffic. 
* Direction is actually irrelevant to the polling functions, except for the Alert Card text. yes, basic error catch of input comboAlgo form. 
* ts of Alert Card is just latest ts from perp_metrics from row it got its last trigger; "17:19:00 UTC 2025-11-22" (time first) in small font on top of Card, then big font: Direction "Long" or "Short" (green or red. Only part of card colored), then medium front: Symbol, then small : comboAlgo.
* polling fetch only the latest perp_metrics data per symbol_exchange; per comboAlgo that was input. ALL conditions must be met on that ts row in perp_metrics per algo, and both/all algos triggered on that ts for comboAlgo trigger for Alert. Clear??  
* "1st Alert Delay", apply per symbol per alert combo of course. 

A) "MT" IS a valid alert (algo) symbol. It is not a trade symbol. In other files we call these: algoSymbol and tradeSymbol; use that naming here. MT is allowed as a single manual input algoSymbol. It is omitted from 'All' of algoSymbol, and totally omitted from tradeSymbol. B) Algos can have different exchanges, i.e. ETH,BTC,XRP;Long;MT_bin_rsi5_chg_10m>20 + BTC_okx_v_chg_5m>30 + All_byb_oi_chg_10m>0.4. re-review your dbsetup. you see that e.g. symbol XRP would have three ts columns all same ts, but one for each exchange. So polling functions have to call ts.symbol.exchange.param... etc. for each part of a comboAlgo, Do you understand? 
C) **Important:  if 'All' is used for algoSymbol, functions must couple each symbol to its counterpart of tradeSymbol, and poll for Alert only for that one symbol. Each Alert is only for one symbol. 

* front end is served other functions by @server.js . Review and determine how to form a new API for perp_metrics in there? 

AI Qs answered:
#1 previous AI suggested card-poll.js should be in backend for local machine resources, vs frontend for browser processing?? consider most modern machines at intel i5 2.5 or so with 8-16G Ram... thats why you are viewing - where best to put parsing and polling and generate Alert functions?
#2 browser refresh should not lose Users settings or previous Cards, or Delay settings. If Stopped then "Start" button acts as semi-refresh and only adds new Alerts in top. **Lets add button (next to maxDisplay alerts):"Wipe" with popup warning. That wipes all memory and Alerts. 

#3 main control buttons toggle: Of course- switching back to View DB, restore selectors/pagination state from appState.
#4 Yes, show inline error in input box. For no triggers, show "No alerts yet" placeholder; yes common sense, common practice here. 
#5 Grid "20 down" for pruning—prune default if User leaves max Alerts blank. Grid top gets auto-populated anytime new Alert. Let User scroll anytime freely.
#6 All is only allowed on symbols. exchanges will only be one at a time; and param, operator, value; all just one entry, not array or multiple. And, symbols is of course taken from perp_metrics symbol column.
#7 You can put grey shade placeholder text in input box: "e.g. All;Short;MT_bin_rsi1_chg_10m<30 + All_bin_v_chg_5m>20" But script wont poll and "Start" unless User puts something in box. 


A) Time Format is time first/then date please. B) ensure api is being polled EVERY 1 minute for any triggering event. C) maxDelay allows 1-100 min. D) All;Short;MT_bin_rsi1_chg_10m<30 + All_bin_v_chg_5m>20 would have to have 2 triggers to validate Alert: any symbol (sans MT) in table, hitting the 'v_chg' threshold, plus the MT symbol hitting the rsi1 - coupled only to itself as tradeSymbol - issue Alert for that tradeSymbol. functions must allow MT treated outside the coupling function. (in the future we may allow BTC and ETH to also be allowed singular algoSymbol entry without coupling). All;Short;SOL,ETH_bin_rsi1_chg_10m<30 + XRP,DOGE,SOL_bin_v_chg_5m>20 would throw error: "cannot couple algoSymbol to tradeSymbol." But SOL,XRP;Short;MT_bin_rsi1_chg_10m<30 + SOL,XRP_bin_v_chg_5m>20 would be fine. (in future, may also create "groups" as in MemeCoin group, that is specific group of symbols being treated as an 'All'.)  *ok, prepare to issue 2 new full files, plus snippets - with exact placement instructs, for - server.js (server0), index.html, style.css, controls.js etc. *First confirm you understand this last notes, and any Qs?
