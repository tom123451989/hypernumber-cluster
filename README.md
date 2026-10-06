# HyperNumber Distributed Cluster Companion

This is a standalone, multi-phone clustering companion project designed to coordinate multiple Android phones (or browser devices) to compute HyperNumber generation in parallel over a local Wi-Fi network.

## 🚀 How to Run:

1. **Install Dependencies:**
   ```bash
   cd cluster-companion
   npm install
   ```

2. **Start the Master Cluster Server:**
   ```bash
   npm start
   ```
   *(This starts the WebSocket orchestration server on port `3001`)*

3. **Connect Master & Worker Phones:**
   - **Phone 1 (Master Controller):** Open `http://<YOUR_PC_IP>:3001` in a browser or phone, leave mode on **Master Controller**, enter your target total (e.g., 1,000,000,000), and click **START CLUSTER COMPUTE**.
   - **Phone 2, 3, etc. (Worker Compute Nodes):** Open `http://<YOUR_PC_IP>:3001`, switch mode to **Worker Compute Phone**, update your WebSocket server URL to match your PC's local IP address (`ws://<YOUR_PC_IP>:3001`), and click **Connect** & **Register as Compute Worker**.

The master dashboard will automatically partition the workload into equal shards across all connected phone workers and stream combined telemetry live!
