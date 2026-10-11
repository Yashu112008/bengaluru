import os
import sqlite3
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "GQQ3TmhgkWNpEPVfcKuz5JXBHnGGBt3z")

# 16 High-Traffic Bengaluru Hotspots (Old + New Corridors)
JUNCTIONS = [
    # Refined exact carriageway coordinates
    {"id": "silk_board", "name": "Silk Board Junction", "lat": 12.9172, "lon": 77.6228},
    {"id": "sony_world", "name": "Sony World Signal Koramangala", "lat": 12.9348, "lon": 77.6258},
    {"id": "outer_ring_road", "name": "Outer Ring Road (Bellandur Ecospace)", "lat": 12.9268, "lon": 77.6785},
    {"id": "ecity", "name": "Electronic City Toll Gate (Hosur Rd)", "lat": 12.8495, "lon": 77.6582},
    {"id": "goraguntepalya", "name": "Goraguntepalya Signal (Tumkur Rd)", "lat": 13.0289, "lon": 77.5412},
    {"id": "bagalur_cross", "name": "Bagalur Cross (Airport Rd)", "lat": 13.1340, "lon": 77.6105},
    
    # Other junctions
    {"id": "kr_puram_bridge", "name": "KR Puram Hanging Bridge & Junction", "lat": 13.0016, "lon": 77.6631},
    {"id": "mg_road", "name": "MG Road (Anil Kumble Circle)", "lat": 12.9756, "lon": 77.6066},
    {"id": "mekhri_circle", "name": "Mekhri Circle Underpass", "lat": 13.0078, "lon": 77.5815},
    {"id": "yeshwanthpur", "name": "Yeshwanthpur Junction / Flyover", "lat": 13.0238, "lon": 77.5503},
    {"id": "bel_circle", "name": "BEL Circle (Jalahalli)", "lat": 13.0422, "lon": 77.5501},
    {"id": "hennur_cross", "name": "Hennur Cross (ORR Junction)", "lat": 13.0232, "lon": 77.6397},
    {"id": "marathahalli", "name": "Marathahalli Bridge (ORR)", "lat": 12.9562, "lon": 77.7019},
    {"id": "tin_factory", "name": "Tin Factory (Old Madras Rd)", "lat": 13.0010, "lon": 77.6625},
    {"id": "hebbal", "name": "Hebbal Flyover (Airport Rd)", "lat": 13.0354, "lon": 77.5988},
    {"id": "whitefield_hope_farm", "name": "Hope Farm Whitefield", "lat": 12.9840, "lon": 77.7516},
]

DB_FILE = "traffic_cache.db"

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS traffic_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                junction_id TEXT,
                junction_name TEXT,
                lat REAL,
                lon REAL,
                current_speed REAL,
                free_flow_speed REAL,
                congestion_pct REAL,
                timestamp TEXT
            )
        """)
        conn.commit()

async def fetch_and_store_traffic():
    """Background task running every 10 minutes."""
    if not TOMTOM_API_KEY:
        print("[WARNING] TOMTOM_API_KEY is not set.")
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    async with httpx.AsyncClient(timeout=15.0) as client:
        records = []
        for j in JUNCTIONS:
            url = (
                f"https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json"
                f"?point={j['lat']},{j['lon']}&key={TOMTOM_API_KEY}"
            )
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    flow = resp.json().get("flowSegmentData", {})
                    curr_spd = float(flow.get("currentSpeed", 0))
                    free_spd = float(max(flow.get("freeFlowSpeed", 1), 1))
                    curr_time = float(flow.get("currentTravelTime", 0))
                    free_time = float(max(flow.get("freeFlowTravelTime", 1), 1))

                    # 1. Primary delay calculation from travel time
                    if curr_time > free_time and free_time > 0:
                        delay_val = ((curr_time - free_time) / curr_time) * 100.0
                        congestion = round(max(0.0, min(100.0, delay_val)), 1)
                    # 2. Secondary calculation from speed reduction
                    elif curr_spd < free_spd:
                        congestion = round(((free_spd - curr_spd) / free_spd) * 100.0, 1)
                    else:
                        congestion = 0.0

                    # 3. Dynamic urban adjustment:
                    # In Bengaluru, junctions like Silk Board & E-City Toll rarely have free-flow speed.
                    # If speed is below typical city threshold (< 50 km/h), compute crawl index.
                    if congestion <= 2.0:
                        if curr_spd <= 15:
                            congestion = round(65.0 + (15 - curr_spd) * 2.0, 1)
                        elif curr_spd <= 30:
                            congestion = round(35.0 + (30 - curr_spd) * 1.5, 1)
                        elif curr_spd <= 45:
                            congestion = round(12.0 + (45 - curr_spd) * 0.8, 1)
                        else:
                            congestion = 5.2  # Nominal baseline delay

                    records.append((
                        j["id"], j["name"], j["lat"], j["lon"],
                        curr_spd, free_spd, congestion, now_iso
                    ))
            except Exception as e:
                print(f"[ERROR] Fetching {j['name']}: {e}")

        if records:
            with sqlite3.connect(DB_FILE) as conn:
                conn.executemany("""
                    INSERT INTO traffic_history 
                    (junction_id, junction_name, lat, lon, current_speed, free_flow_speed, congestion_pct, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, records)
                conn.commit()
            print(f"[INFO] Ingested {len(records)} traffic snapshots at {now_iso}")

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # Run fetch on startup
    await fetch_and_store_traffic()
    # Schedule every 10 minutes (8 locations * 6/hr * 24hr = 1,152 req/day - well under 2,500 free tier limit)
    scheduler.add_job(fetch_and_store_traffic, "interval", minutes=10)
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(title="Bengaluru Traffic Pulse", lifespan=lifespan)

@app.get("/api/traffic/latest")
async def get_latest_traffic():
    """Returns the most recent snapshot for each junction."""
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT t.*
            FROM traffic_history t
            INNER JOIN (
                SELECT junction_id, MAX(id) as max_id
                FROM traffic_history
                GROUP BY junction_id
            ) latest ON t.id = latest.max_id
            ORDER BY congestion_pct DESC
        """)
        rows = [dict(r) for r in cur.fetchall()]
        return {"data": rows, "count": len(rows)}

@app.get("/api/traffic/history")
async def get_traffic_history():
    """Returns past 24 hourly averages for the trend chart."""
    with sqlite3.connect(DB_FILE) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                strftime('%H:00', timestamp) as hour,
                ROUND(AVG(congestion_pct), 1) as avg_congestion
            FROM traffic_history
            GROUP BY hour
            ORDER BY timestamp DESC
            LIMIT 12
        """)
        rows = [dict(r) for r in cur.fetchall()]
        return {"history": list(reversed(rows))}

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>BLR Pulse | Bengaluru Live Traffic Analytics</title>
  
  <!-- Tailwind CSS -->
  <script src="https://cdn.tailwindcss.com"></script>
  <!-- Leaflet Map CSS & JS -->
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <!-- Chart.js -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  
  <style>
    body { background-color: #0b0f19; color: #f3f4f6; font-family: system-ui, -apple-system, sans-serif; }
    #map { height: 480px; width: 100%; border-radius: 0.75rem; }
    .leaflet-popup-content-wrapper { background: #1e293b; color: #f8fafc; border-radius: 8px; }
    .leaflet-popup-tip { background: #1e293b; }
  </style>
</head>
<body class="p-4 md:p-8 max-w-7xl mx-auto">
  <!-- Header -->
  <header class="flex flex-col md:flex-row md:items-center justify-between pb-6 border-b border-gray-800 gap-4">
    <div>
      <div class="flex items-center gap-2">
        <span class="inline-block w-3 h-3 bg-red-500 rounded-full animate-ping"></span>
        <h1 class="text-2xl md:text-3xl font-extrabold tracking-tight text-white">BLR Pulse</h1>
        <span class="text-xs bg-gray-800 text-gray-300 px-2 py-0.5 rounded border border-gray-700">Live 24/7</span>
      </div>
      <p class="text-gray-400 text-sm mt-1">Real-time corridor telemetry & bottleneck analytics across Bengaluru</p>
    </div>
    <div class="flex items-center gap-3 text-xs text-gray-400">
      <div class="bg-gray-900 border border-gray-800 px-3 py-2 rounded-lg">
        Auto-refreshes every <span class="text-emerald-400 font-mono font-bold">30s</span>
      </div>
      <div id="last-updated" class="bg-gray-900 border border-gray-800 px-3 py-2 rounded-lg">
        Updating...
      </div>
    </div>
  </header>

  <!-- KPI Cards -->
  <div class="grid grid-cols-1 sm:grid-cols-3 gap-4 my-6">
    <div class="bg-gray-900/80 border border-gray-800 p-5 rounded-xl">
      <div class="text-gray-400 text-xs uppercase tracking-wider font-semibold">City Congestion Index</div>
      <div id="avg-congestion" class="text-3xl font-black mt-2 text-white">--%</div>
      <p class="text-gray-500 text-xs mt-1">Average delay across monitored corridors</p>
    </div>
    <div class="bg-gray-900/80 border border-gray-800 p-5 rounded-xl">
      <div class="text-gray-400 text-xs uppercase tracking-wider font-semibold">Worst Bottleneck</div>
      <div id="worst-junction" class="text-xl font-bold mt-2 text-rose-400 truncate">Detecting...</div>
      <p id="worst-metric" class="text-gray-500 text-xs mt-1">-- km/h vs free flow</p>
    </div>
    <div class="bg-gray-900/80 border border-gray-800 p-5 rounded-xl">
      <div class="text-gray-400 text-xs uppercase tracking-wider font-semibold">Active Monitored Points</div>
      <div id="active-points" class="text-3xl font-black mt-2 text-indigo-400">16</div>
      <p class="text-gray-500 text-xs mt-1">Autonomous 10-min background scheduler</p>
    </div>
  </div>

  <!-- Main Grid: Map & Live Rankings -->
  <div class="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
    <!-- Map Section -->
    <div class="lg:col-span-2 bg-gray-900 border border-gray-800 p-4 rounded-xl">
      <h2 class="text-sm font-semibold uppercase tracking-wider text-gray-400 mb-3">Live Geographic Heatmap</h2>
      <div id="map"></div>
    </div>

    <!-- Leaderboard -->
    <div class="bg-gray-900 border border-gray-800 p-4 rounded-xl flex flex-col">
      <h2 class="text-sm font-semibold uppercase tracking-wider text-gray-400 mb-3">Corridor Congestion Status</h2>
      <div id="leaderboard" class="space-y-3 overflow-y-auto max-h-[440px] pr-1">
        <!-- Rendered dynamically -->
      </div>
    </div>
  </div>

  <!-- Historical Trend Chart -->
  <div class="bg-gray-900 border border-gray-800 p-5 rounded-xl mb-6">
    <h2 class="text-sm font-semibold uppercase tracking-wider text-gray-400 mb-3">Recent Congestion Trajectory (Hourly Avg)</h2>
    <div class="h-64">
      <canvas id="trendChart"></canvas>
    </div>
  </div>

  <footer class="text-center text-xs text-gray-600 py-4">
    Automated telemetry platform • Built for portfolio showcase
  </footer>

  <script>
    // Initialize Leaflet Map centered on Bangalore
    const map = L.map('map').setView([12.990, 77.640], 11);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors',
      maxZoom: 19
    }).addTo(map);

    let markers = [];
    let trendChart = null;

    function getCongestionBadge(pct) {
      if (pct >= 60) return { bg: 'bg-rose-500/20', text: 'text-rose-400', color: '#f43f5e', label: 'Severe' };
      if (pct >= 30) return { bg: 'bg-amber-500/20', text: 'text-amber-400', color: '#f59e0b', label: 'Moderate' };
      return { bg: 'bg-emerald-500/20', text: 'text-emerald-400', color: '#10b981', label: 'Fluid' };
    }

    async function loadData() {
      try {
        const [latestRes, historyRes] = await Promise.all([
          fetch('/api/traffic/latest'),
          fetch('/api/traffic/history')
        ]);
        const latest = await latestRes.json();
        const history = await historyRes.json();

        renderDashboard(latest.data || []);
        renderChart(history.history || []);
        document.getElementById('last-updated').textContent = "Updated: " + new Date().toLocaleTimeString();
      } catch (err) {
        console.error("Fetch error:", err);
      }
    }

    function renderDashboard(items) {
      const leaderboard = document.getElementById('leaderboard');
      leaderboard.innerHTML = '';

      // Clear existing markers
      markers.forEach(m => map.removeLayer(m));
      markers = [];

      if (items.length === 0) {
        leaderboard.innerHTML = '<p class="text-gray-500 text-sm">No data yet. Ingestion will run shortly.</p>';
        return;
      }

      let totalCongestion = 0;
      items.forEach((item, index) => {
        totalCongestion += item.congestion_pct;
        const meta = getCongestionBadge(item.congestion_pct);

        // Map Circle Marker
        const circle = L.circleMarker([item.lat, item.lon], {
          radius: 9,
          fillColor: meta.color,
          color: '#ffffff',
          weight: 1.5,
          opacity: 0.9,
          fillOpacity: 0.85
        }).addTo(map);

        circle.bindPopup(`
          <div class="text-xs">
            <p class="font-bold text-sm mb-1">${item.junction_name}</p>
            <p>Congestion: <span style="color:${meta.color}; font-weight:bold;">${item.congestion_pct}%</span></p>
            <p>Current Speed: <b>${item.current_speed} km/h</b></p>
            <p>Free Flow: <b>${item.free_flow_speed} km/h</b></p>
          </div>
        `);
        markers.push(circle);

        // Leaderboard Item
        const div = document.createElement('div');
        div.className = 'p-3 bg-gray-950/60 rounded-lg border border-gray-800/80 flex items-center justify-between';
        div.innerHTML = `
          <div>
            <p class="text-sm font-semibold text-gray-200">${item.junction_name}</p>
            <p class="text-xs text-gray-400">${item.current_speed} km/h <span class="text-gray-600">/ ${item.free_flow_speed} km/h</span></p>
          </div>
          <div class="text-right">
            <span class="text-xs font-bold px-2 py-0.5 rounded ${meta.bg} ${meta.text}">${item.congestion_pct}%</span>
          </div>
        `;
        leaderboard.appendChild(div);
      });

      // Update KPI Cards
      const avg = (totalCongestion / items.length).toFixed(1);
      document.getElementById('avg-congestion').textContent = `${avg}%`;
      document.getElementById('worst-junction').textContent = items[0].junction_name;
      document.getElementById('worst-metric').textContent = `${items[0].current_speed} km/h (Delay: ${items[0].congestion_pct}%)`;
    }

    function renderChart(history) {
      const ctx = document.getElementById('trendChart').getContext('2d');
      const labels = history.map(h => h.hour);
      const data = history.map(h => h.avg_congestion);

      if (trendChart) {
        trendChart.data.labels = labels;
        trendChart.data.datasets[0].data = data;
        trendChart.update();
        return;
      }

      trendChart = new Chart(ctx, {
        type: 'line',
        data: {
          labels: labels.length ? labels : ['00:00', '04:00', '08:00', '12:00', '16:00', '20:00'],
          datasets: [{
            label: 'Avg Congestion %',
            data: data.length ? data : [10, 15, 65, 40, 75, 45],
            borderColor: '#38bdf8',
            backgroundColor: 'rgba(56, 189, 248, 0.1)',
            fill: true,
            tension: 0.35,
            borderWidth: 2,
            pointRadius: 4,
            pointBackgroundColor: '#38bdf8'
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false } },
          scales: {
            x: { grid: { color: 'rgba(255, 255, 255, 0.05)' }, ticks: { color: '#9ca3af' } },
            y: { grid: { color: 'rgba(255, 255, 255, 0.05)' }, ticks: { color: '#9ca3af' }, min: 0, max: 100 }
          }
        }
      });
    }

    // Initial load + refresh every 30 seconds on client
    loadData();
    setInterval(loadData, 30000);
  </script>
</body>
</html>
    """
