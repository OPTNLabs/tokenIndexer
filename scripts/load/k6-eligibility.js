import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE = __ENV.BASE_URL || 'http://127.0.0.1:8080';
const CATEGORY = __ENV.CATEGORY || '';
const LOCKING = __ENV.LOCKING || '';

export const options = {
  scenarios: {
    steady: {
      executor: 'ramping-arrival-rate',
      startRate: 100,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 800,
      stages: [
        { target: 300, duration: '2m' },
        { target: 800, duration: '3m' },
        { target: 1000, duration: '2m' }
      ]
    }
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<150'],
  },
};

export default function () {
  const url = `${BASE}/v1/token/${CATEGORY}/holder/${LOCKING}`;
  const res = http.get(url, { headers: { 'Accept-Encoding': 'gzip' } });
  check(res, {
    'status is 200': (r) => r.status === 200,
    'has eligible field': (r) => r.body.includes('eligible'),
  });
  sleep(Math.random() * 0.2);
}
