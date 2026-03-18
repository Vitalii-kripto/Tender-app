import { spawn } from 'child_process';
import fs from 'fs';
import path from 'path';

console.log('🚀 run_app.js is starting!');

const npmCmd = process.platform === 'win32' ? 'npm.cmd' : 'npm';
console.log(`🚀 Starting Frontend (${npmCmd} run dev:frontend)...`);

const frontend = spawn(npmCmd, ['run', 'dev:frontend'], {
  stdio: 'inherit',
  shell: true
});

const venvPython = '.\\\\.venv\\\\Scripts\\\\python.exe';
const venvPythonPath = path.join(process.cwd(), '.venv', 'Scripts', 'python.exe');

if (!fs.existsSync(venvPythonPath)) {
  console.error('Не найден Python виртуального окружения .venv. Сначала создайте/установите зависимости в виртуальное окружение.');
  process.exit(1);
}

console.log(`🚀 Starting Backend (${venvPython} run_backend.py)...`);

const backend = spawn(venvPython, ['run_backend.py'], {
  stdio: 'inherit',
  shell: true
});

process.on('SIGINT', () => {
  frontend.kill('SIGINT');
  backend.kill('SIGINT');
  process.exit();
});
