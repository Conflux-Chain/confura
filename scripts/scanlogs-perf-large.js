import http from 'k6/http';
import { check } from 'k6';

const duration = __ENV.SCANLOGS_PERF_P5_DURATION || '5m';
const vus = Number(__ENV.SCANLOGS_PERF_P5_VUS || 4);
const request = JSON.parse(__ENV.SCANLOGS_PERF_P5_REQUEST);

export const options = {
  vus,
  duration,
  discardResponseBodies: false,
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  thresholds: {
    checks: ['rate==1'],
    http_req_failed: ['rate==0'],
  },
};

export default function () {
  const response = http.post(__ENV.SCANLOGS_ETH_RPC, JSON.stringify({
    jsonrpc: '2.0',
    id: `${__VU}-${__ITER}`,
    method: 'eth_scanLogs',
    params: [request],
  }), {
    headers: { 'Content-Type': 'application/json' },
    timeout: `${__ENV.SCANLOGS_RPC_TIMEOUT_SECONDS || 15}s`,
  });

  let body;
  try {
    body = response.json();
  } catch (_) {
    body = null;
  }
  check(response, {
    'HTTP status is 200': (r) => r.status === 200,
    'JSON-RPC result has no error': () => body !== null && body.error == null,
    'large page contains 1000 logs': () => body !== null && body.result != null &&
      Array.isArray(body.result.logs) && body.result.logs.length === 1000,
    'large page has a next cursor': () => body !== null && body.result != null &&
      body.result.nextCursor != null,
  });
}
