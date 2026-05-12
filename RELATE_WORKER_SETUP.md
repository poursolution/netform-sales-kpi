# Relate 공용 API 연동 설정

이 저장소의 `worker/relate-proxy.js`는 Relate API 키를 브라우저에 노출하지 않고, 매출 대시보드가 공용으로 읽을 수 있는 파이프라인 데이터만 반환합니다.

## 배포 순서

1. Cloudflare Workers에 로그인합니다.

```bash
npx wrangler login
```

2. Relate API 키를 Worker secret으로 등록합니다.

```bash
npx wrangler secret put RELATE_API_KEY
```

3. Worker를 배포합니다.

```bash
npx wrangler deploy
```

4. 배포된 Worker URL을 복사합니다.

예시:

```text
https://netform-sales-kpi-api.promieses.workers.dev
```

5. 매출통합 대시보드에서 `Relate 연동`을 열고 `공용 연동 주소`에 Worker URL을 입력합니다.

## 동작 방식

```text
Relate API → Cloudflare Worker → 매출통합 대시보드 → KPI 관리도구
```

- 직원별 브라우저에 Relate API 키를 저장하지 않습니다.
- KPI 관리도구는 매출통합 대시보드가 만든 자동 KPI 스냅샷을 읽습니다.
- Worker 응답은 5분 캐시됩니다. 수동 동기화 시 `refresh=1`로 캐시를 우회합니다.

## 보안 메모

이 구조는 Relate API 키를 숨기는 구조입니다. 다만 Worker URL을 아는 사람은 변환된 파이프라인 데이터를 볼 수 있습니다. 외부 공개를 막아야 하면 Cloudflare Access 같은 접근제어를 추가해야 합니다.
