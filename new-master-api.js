// master-api.js
const { spawn } = require("child_process");
const path = require("path");

// Scripts
const historicScripts = [
  "oi-h.js",
  "ohlcv-h.js",
  "fr-h.js",
  "pfr-h.js"
  // LSR-h excluded, run separately
];

const lsrScript = "all-lsr-h.js"; // run last, by itself
const continuousScripts = [
  "oi-c.js",
  "ohlcv-c.js",
  "fr-c.js",
  "pfr-c.js",
  "lsr-c.js",
  "lq-c.js"
];

function runScript(script) {
  return new Promise((resolve, reject) => {
    const proc = spawn("node", [path.join("test", script)], {
      stdio: "inherit"
    });

    proc.on("close", code => {
      if (code !== 0) {
        reject(new Error(`${script} exited with code ${code}`));
      } else {
        resolve();
      }
    });
  });
}

async function main() {
  console.log("🚀 Starting historic scripts (parallel, except LSR)...");
  await Promise.all(historicScripts.map(runScript));

  console.log("🚀 Running all-lsr-h.js separately (last)...");
  await runScript(lsrScript);

  console.log("🚀 Starting continuous scripts...");
  for (const script of continuousScripts) {
    runScript(script).catch(err =>
      console.error(`❌ Error in ${script}:`, err.message)
    );
  }
}

if (require.main === module) {
  main();
}


