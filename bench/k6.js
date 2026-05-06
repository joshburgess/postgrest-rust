// k6 benchmark script for pg-rest-server.
//
// Prerequisites:
//   docker compose up -d
//   cargo run -p pg-rest-server-tokio-postgres --release -- --config test/fixtures/test-config.toml
//   # ...or pg-rest-server-resolute for the resolute backend
//
// Run:
//   k6 run bench/k6.js
//   k6 run --vus 50 --duration 30s bench/k6.js

import http from "k6/http";
import { check, sleep } from "k6";

const BASE = __ENV.BASE_URL || "http://localhost:3001";
const TOKEN = __ENV.JWT_TOKEN || "";

const headers = TOKEN
  ? { Authorization: `Bearer ${TOKEN}`, "Content-Type": "application/json" }
  : { "Content-Type": "application/json" };

export const options = {
  stages: [
    { duration: "5s", target: 10 },
    { duration: "20s", target: 50 },
    { duration: "5s", target: 0 },
  ],
  thresholds: {
    http_req_duration: ["p(95)<100"], // 95th percentile under 100ms
    http_req_failed: ["rate<0.01"],
  },
};

export default function () {
  // Simple read
  const r1 = http.get(`${BASE}/authors`, { headers });
  check(r1, {
    "read authors 200": (r) => r.status === 200,
    "read authors has data": (r) => JSON.parse(r.body).length >= 3,
  });

  // Read with filter
  const r2 = http.get(`${BASE}/books?pages=gt.300&order=title.asc`, {
    headers,
  });
  check(r2, {
    "filtered read 200": (r) => r.status === 200,
  });

  // Read with embedding
  const r3 = http.get(`${BASE}/authors?select=name,books(title)`, { headers });
  check(r3, {
    "embed read 200": (r) => r.status === 200,
  });

  // RPC call
  const r4 = http.post(
    `${BASE}/rpc/add`,
    JSON.stringify({ a: 1, b: 2 }),
    { headers }
  );
  check(r4, {
    "rpc 200": (r) => r.status === 200,
  });

  // OpenAPI spec
  const r5 = http.get(`${BASE}/`, { headers });
  check(r5, {
    "openapi 200": (r) => r.status === 200,
  });

  sleep(0.01);
}
