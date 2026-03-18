import { spawn } from 'child_process';
import os from 'os';

console.log("🚀 run_app.js is starting!");

const isWin = os.platform() === 'win32';
const pythonCmd = isWin ? 'python' : 'python3';
const npmCmd = isWin ? 'npm.cmd' : 'npm';

function startService(command, args, name) {
    console.log(`🚀 Starting ${name} (${command} ${args.join(' ')})...`);
    const proc = spawn(command, args, {
        stdio: 'inherit',
        shell: isWin
    });

    proc.on('error', (err) => {
        console.error(`❌ Failed to start ${name}:`, err);
    });

    return proc;
}

const frontend = startService(npmCmd, ['run', 'dev:frontend'], 'Frontend');
const backend = startService(pythonCmd, ['run_backend.py'], 'Backend');

process.on('SIGINT', () => {
    console.log("\n🛑 Stopping services...");
    frontend.kill();
    backend.kill();
    process.exit();
});
