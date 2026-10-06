import express from 'express';
import http from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import cors from 'cors';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

interface JobConfig {
  targetTotal: number;
  startingHn: string;
  startingOp: string;
}

interface WorkerNode {
  ws: WebSocket;
  id: string;
  name: string;
  shardStart: number;
  shardEnd: number;
  currentGenerated: number;
  speed: number;
  status: 'IDLE' | 'RUNNING' | 'COMPLETED';
}

let activeJob: JobConfig | null = null;
const workers = new Map<WebSocket, WorkerNode>();

wss.on('connection', (ws) => {
  const workerId = Math.random().toString(36).slice(2, 8);
  console.log(`Worker connected: ${workerId}`);

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.type === 'REGISTER_WORKER') {
        workers.set(ws, {
          ws,
          id: workerId,
          name: msg.name || `Phone Worker ${workerId}`,
          shardStart: 0,
          shardEnd: 0,
          currentGenerated: 0,
          speed: 0,
          status: 'IDLE',
        });
        broadcastClusterState();
      } else if (msg.type === 'HEARTBEAT') {
        const worker = workers.get(ws);
        if (worker) {
          worker.currentGenerated = msg.generated || 0;
          worker.speed = msg.speed || 0;
          worker.status = msg.status || 'RUNNING';
          broadcastClusterState();
        }
      } else if (msg.type === 'START_JOB') {
        activeJob = {
          targetTotal: msg.targetTotal || 1000000000,
          startingHn: msg.startingHn || 'L4A',
          startingOp: msg.startingOp || 'EexSq499914209',
        };

        // Ensure master is also registered as a worker so Master contributes compute too
        if (!workers.has(ws)) {
          workers.set(ws, {
            ws,
            id: workerId,
            name: `Master (Controller + Worker)`,
            shardStart: 0,
            shardEnd: 0,
            currentGenerated: 0,
            speed: 0,
            status: 'IDLE',
          });
        }

        const workerArray = Array.from(workers.values());
        const totalWorkers = workerArray.length;
        if (totalWorkers > 0) {
          const shardSize = Math.ceil(activeJob.targetTotal / totalWorkers);
          workerArray.forEach((w, idx) => {
            w.shardStart = idx * shardSize + 1;
            w.shardEnd = Math.min(activeJob!.targetTotal, (idx + 1) * shardSize);
            w.currentGenerated = 0;
            w.status = 'RUNNING';

            w.ws.send(JSON.stringify({
              type: 'ASSIGN_SHARD',
              shardStart: w.shardStart,
              shardEnd: w.shardEnd,
              startingHn: activeJob!.startingHn,
              startingOp: activeJob!.startingOp,
            }));
          });
        }
        broadcastClusterState();
      } else if (msg.type === 'STOP_JOB') {
        activeJob = null;
        workers.forEach((w) => {
          w.status = 'IDLE';
          w.ws.send(JSON.stringify({ type: 'STOP' }));
        });
        broadcastClusterState();
      }
    } catch (e) {
      console.error('Error handling message', e);
    }
  });

  ws.on('close', () => {
    workers.delete(ws);
    console.log(`Worker disconnected: ${workerId}`);
    broadcastClusterState();
  });
});

function broadcastClusterState() {
  const workerList = Array.from(workers.values()).map(w => ({
    id: w.id,
    name: w.name,
    shardStart: w.shardStart,
    shardEnd: w.shardEnd,
    currentGenerated: w.currentGenerated,
    speed: w.speed,
    status: w.status,
  }));

  const totalGenerated = workerList.reduce((acc, w) => acc + w.currentGenerated, 0);
  const totalSpeed = workerList.reduce((acc, w) => acc + w.speed, 0);

  const stateMsg = JSON.stringify({
    type: 'CLUSTER_STATE',
    activeJob,
    workers: workerList,
    totalGenerated,
    totalSpeed,
  });

  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(stateMsg);
    }
  });
}

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`HyperNumber Cluster Master Server running on port ${PORT}`);
});
