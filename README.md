# CCTV Local Recording & Event Publisher Worker

A lightweight, offline-resilient CCTV video recorder and metadata event publisher built with **Golang** for the Way Kambas Wildlife Surveillance System.

---

## 📋 Overview & Architecture

This worker service runs locally on a low-spec edge machine (e.g. AMD A6, 4GB RAM on Debian/Ubuntu in Proxmox LXC) and performs:
1. **Camera Bootstrap & Live MQTT Sync**: Fetches initial active CCTV cameras/devices from the central REST API, then listens to real-time MQTT events (`UPSERT_CAMERA`, `REMOVE_CAMERA`, `SYNC_ALL`) to dynamically start, stop, or update recorder sessions without restarts.
2. **Segmented MP4 Recording (0% CPU Re-encode Overhead)**: Uses FFmpeg bitstream passthrough (`-c copy`) to segment RTSP streams into 5-minute video chunks (`%Y-%m-%d_%H-%M-%S.mp4`).
3. **Completion Detection & RabbitMQ Event Publishing**: Automatically handles finished MP4 segments via an async worker pool, uploads them (optional S3/MinIO), and publishes persistent `RECORDING_COMPLETED` JSON events to RabbitMQ.
4. **Resilience & Auto-reconnect**: Automatically reconnects if camera RTSP streams, MQTT broker, or RabbitMQ network drops.
5. **Graceful Shutdown**: Traps `SIGINT`/`SIGTERM` to safely terminate child FFmpeg processes without corrupting active MP4 recordings.

```
[ Backend REST API / Frontend ] 
         │ (Initial bootstrap & CRUD events)
         ├── REST API (GET /devices/worker/cameras)
         └── MQTT Broker (workers/<worker_id>/events, workers/events)
                   │
                   ▼
       [ worker-record (Golang) ]
         ├── Live Camera Consumer (Upsert / Remove / Sync)
         ├── Direct Chunk FFmpeg (-c copy) ──► Local Storage (/opt/recordings/queue/<deviceId>/)
         └── Async Upload Pool ──────────────► MinIO / S3 Storage
                                               └──► RabbitMQ (RECORDING_COMPLETED Event)
```

---

## ⚙️ Requirements

- **Linux OS**: Debian 11/12, Ubuntu 20.04/22.04/24.04 (bare metal or Proxmox LXC)
- **FFmpeg**: Version 4.x or higher (`apt install -y ffmpeg`)
- **Go**: Version 1.21 or higher (only required on the build machine)

---

## 🚀 Environment Variables (`.env`)

Create a `.env` file in the project or installation directory based on `.env.example`:

```env
# Unique identifier for this worker node
WORKER_ID=worker_recorder_01

# Central Backend REST API Endpoint
API_BASE_URL=https://api-kamera.psti-ubl.id/devices
API_KEY_HEADER=x-api-key
API_KEY=
API_AUTH_TOKEN=

# RabbitMQ Broker Configuration
RMQ_HOST=127.0.0.1
RMQ_USER=guest
RMQ_PASS=guest
RMQ_PORT=5672
RMQ_VHOST=/
RABBITMQ_QUEUE_NAME=cctv.recordings
RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5672/

# Local Storage Path for Segmented MP4 Recordings
RECORD_STORAGE_PATH=/opt/recordings/queue

# Public Playback Base URL for generated event URLs
RECORDING_BASE_URL=http://127.0.0.1:9000/recordings

# Segment duration in seconds (300 = 5 minutes)
SEGMENT_DURATION_SECONDS=300

# Scanner / Watcher interval in seconds
SCAN_INTERVAL_SECONDS=5

# Retry interval for auto-reconnect
RETRY_INTERVAL_SECONDS=5

# Optional S3 Upload Toggle
ENABLE_S3_UPLOAD=false

# Path to FFmpeg executable binary
FFMPEG_PATH=ffmpeg
```

---

## 📦 Build & Local Run

### 1. Download Dependencies
```bash
go mod tidy
```

### 2. Run Directly
```bash
go run main.go
```

### 3. Build Binary (for Linux AMD64 from any OS)
```bash
# On Linux / Proxmox:
go build -o worker-record main.go

# Cross-compiling for Linux from Windows:
set GOOS=linux
set GOARCH=amd64
go build -o worker-record main.go
```

---

## 🛠️ Deployment on Linux / Proxmox LXC

### Step 1: Install FFmpeg
```bash
sudo apt update && sudo apt install -y ffmpeg
```

### Step 2: Create Application Directory & Storage Folder
```bash
sudo mkdir -p /opt/worker-record
sudo mkdir -p /opt/recordings/queue
sudo chmod -R 755 /opt/recordings
```

### Step 3: Copy Binary and Configuration
```bash
# Copy binary and .env to /opt/worker-record
sudo cp worker-record /opt/worker-record/
sudo cp .env /opt/worker-record/
sudo chmod +x /opt/worker-record/worker-record
```

### Step 4: Setup and Start Systemd Service
```bash
# Copy systemd unit file
sudo cp camera-recorder.service /etc/systemd/system/

# Reload systemd, enable and start service
sudo systemctl daemon-reload
sudo systemctl enable camera-recorder
sudo systemctl start camera-recorder
```

### Step 5: Check Service Status and Logs
```bash
# Check service status
sudo systemctl status camera-recorder

# View live application logs
sudo journalctl -u camera-recorder -f
```

---

## 📨 RabbitMQ Event Payload Format

When a 5-minute video segment completes, the worker publishes a JSON payload matching the backend `Recording` model:

```json
{
  "event": "RECORDING_COMPLETED",
  "deviceId": "836bf517-4159-4c7e-a994-f64d475c7c00",
  "deviceName": "Kamera Pos 1",
  "macAddress": "F7:BC:FF:A9:F7:52",
  "fileName": "2026-09-01_23-05-00.mp4",
  "path": "/opt/recordings/queue/836bf517-4159-4c7e-a994-f64d475c7c00/2026-09-01_23-05-00.mp4",
  "url": "http://127.0.0.1:9000/recordings/836bf517-4159-4c7e-a994-f64d475c7c00/2026-09-01_23-05-00.mp4",
  "size": 48291040,
  "duration": 300,
  "createdAt": "2026-09-01T23:05:00.000Z"
}
```

---

## 🛑 Graceful Shutdown

To stop the worker gracefully without corrupting current video files:
```bash
sudo systemctl stop camera-recorder
```
The service sends `SIGTERM`, allowing FFmpeg processes to finalize and flush their active recording buffers cleanly before closing.
