const fs = require('fs');
const path = require('path');

class MasterAPI {
    constructor() {
        this.scripts = [];
        this.running = false;
        this.intervals = {};
    }

    async initialize() {
        console.log('🚀 FadeMoe4 Master API Initializing...');
        await this.discoverScripts();
        console.log(`✅ Found ${this.scripts.length} scripts in apis/ folder`);
    }

    async discoverScripts() {
        const apisDir = path.join(__dirname, 'apis');
        
        try {
            const files = fs.readdirSync(apisDir);
            this.scripts = files
                .filter(file => file.endsWith('.js'))
                .map(file => {
                    const fullPath = path.join(apisDir, file);
                    const stats = fs.statSync(fullPath);
                    return {
                        name: file,
                        path: fullPath,
                        mtime: stats.mtime,
                        type: this.determineScriptType(file)
                    };
                })
                .sort((a, b) => a.name.localeCompare(b.name));
        } catch (error) {
            console.error('❌ Error discovering scripts:', error.message);
            this.scripts = [];
        }
    }

    determineScriptType(filename) {
        const parts = filename.replace('.js', '').split('-');
        if (parts.length < 3) return 'unknown';
        
        const source = parts[0];
        const metric = parts[1];
        const mode = parts[2];
        return {
            source: source,
            metric: metric,
            mode: mode === 'h' ? 'history' : mode === 'c' ? 'current' : 'unknown',
            fullName: filename
        };
    }

    async runBackfill() {
        console.log('🔄 Starting Backfill Mode...');
        const historyScripts = this.scripts.filter(script => script.type.mode === 'history');
        
        if (historyScripts.length === 0) {
            console.log('✅ No history scripts found, skipping backfill');
            return;
        }

        console.log(`📊 Running ${historyScripts.length} history scripts...`);
        
        for (const script of historyScripts) {
            try {
                console.log(`\n🚀 Executing ${script.name}...`);
                const scriptModule = require(script.path);
                
                if (typeof scriptModule.execute === 'function') {
                    await scriptModule.execute();
                } else if (typeof scriptModule.fetchData === 'function') {
                    await scriptModule.fetchData();
                } else if (typeof scriptModule.backfill === 'function') {
                    await scriptModule.backfill();
                } else {
                    await require(script.path);
                }
                
                console.log(`✅ Completed ${script.name}`);
                
                await new Promise(resolve => setTimeout(resolve, 2000));
                
            } catch (error) {
                console.error(`❌ Error in ${script.name}:`, error.message);
            }
        }
        
        console.log('🎉 Backfill Mode Completed!');
    }

    startLiveMode() {
        console.log('⚡ Starting Live Mode...');
        const currentScripts = this.scripts.filter(script => script.type.mode === 'current');
        
        if (currentScripts.length === 0) {
            console.log('⚠️ No current scripts found');
            return;
        }

        console.log(`📡 Starting ${currentScripts.length} live data collectors...`);
        
        currentScripts.forEach(script => {
            try {
                const interval = 60000;
                console.log(`🚀 Starting ${script.name} with ${interval/1000}s interval`);
                
                this.intervals[script.name] = setInterval(async () => {
                    try {
                        console.log(`📈 Running ${script.name}...`);
                        const scriptModule = require(script.path);
                        
                        if (typeof scriptModule.execute === 'function') {
                            await scriptModule.execute();
                        } else if (typeof scriptModule.collectLive === 'function') {
                            await scriptModule.collectLive();
                        } else if (typeof scriptModule.run === 'function') {
                            await scriptModule.run();
                        } else {
                            await require(script.path);
                        }
                        
                        console.log(`✅ ${script.name} completed`);
                    } catch (error) {
                        console.error(`❌ Error in ${script.name}:`, error.message);
                    }
                }, interval);
                
            } catch (error) {
                console.error(`❌ Failed to start ${script.name}:`, error.message);
            }
        });
        
        this.running = true;
        console.log('✅ Live Mode Active - All collectors running');
    }

    stopLiveMode() {
        console.log('🛑 Stopping Live Mode...');
        
        Object.keys(this.intervals).forEach(scriptName => {
            clearInterval(this.intervals[scriptName]);
            console.log(`⏹️ Stopped ${scriptName}`);
        });
        
        this.intervals = {};
        this.running = false;
        console.log('✅ Live Mode Stopped');
    }

    async start() {
        await this.initialize();
        
        await this.runBackfill();
        
        this.startLiveMode();
        console.log('\n🎯 Master API Fully Operational!');
        console.log('📊 Backfill completed, live data collection active');
    }

    async stop() {
        console.log('🛑 Shutting down Master API...');
        this.stopLiveMode();
        console.log('👋 Master API Shutdown Complete');
    }
}

if (require.main === module) {
    const master = new MasterAPI();
    
    process.on('SIGINT', async () => {
        console.log('\n⚠️ Shutdown signal received...');
        await master.stop();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.log('\n⚠️ Termination signal received...');
        await master.stop();
        process.exit(0);
    });

    master.start().catch(console.error);
}

module.exports = MasterAPI;