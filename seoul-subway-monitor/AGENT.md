# 프로젝트 목표: 서울 지하철 실시간 열차 위치 기반 모니터링 및 분석 시스템 구축

## 1. 개발 환경 및 기술 스택
- **Language**: Python 3.10+
- **Database**: Supabase (PostgreSQL)
- **Library**: requests, supabase-py, python-dotenv, schedule
- **Project Name**: `seoul-subway-monitor`

## 2. 표준 프로젝트 폴더 구조
모든 팀원은 반드시 아래 구조를 유지합니다.
- `seoul-subway-monitor/`
    - `src/` (소스 코드)
        - `config.py`: API 키 및 데이터베이스(DB) 접속 정보 관리
        - `api_client.py`: 서울시 OPEN API 연동 및 실시간 데이터 수집
        - `db_client.py`: Supabase 데이터 변환 및 적재(Ingestion) 로직
        - `main.py`: 메인 배치 프로세스 및 스케줄러 실행 모듈
    - `docs/` (문서)
        - `schema.sql`: 데이터베이스 테이블 생성 스키마 SQL
        - `analysis_plan.md`: 데이터 분석 상세 계획 및 시나리오
    - `.env`: API Key 등 보안이 필요한 환경 변수 관리 파일
    - `requirements.txt`: 프로젝트에 필요한 Python 라이브러리 의존성 목록

## 3. 데이터베이스(Supabase) 표준 스키마
테이블명: `realtime_subway_positions`
API의 원본 컬럼명을 아래의 직관적인 `snake_case` 영문명으로 통일합니다.

- `subwayId` -> `line_id` (지하철 호선 ID)
- `subwayNm` -> `line_name` (지하철 호선명)
- `statnId` -> `station_id` (지하철 역 ID)
- `statnNm` -> `station_name` (지하철 역명)
- `trainNo` -> `train_number` (열차 번호)
- `lastRecptnDt` -> `last_rec_date` (최종 수신 날짜)
- `recptnDt` -> `last_rec_time` (최종 수신 시간)
- `updnLine` -> `direction_type` (0:상행/내선, 1:하행/외선)
- `statnTid` -> `dest_station_id` (종착역 ID)
- `statnTnm` -> `dest_station_name` (종착역명)
- `trainSttus` -> `train_status` (0:진입, 1:도착, 2:출발, 3:전전역출발 등)
- `directAt` -> `is_express` (1:급행, 0:아님, 7:특급)
- `lstcarAt` -> `is_last_train` (Boolean 변환)
- (추가) `created_at` -> `created_at` (데이터 적재 시간, Default: now())

## 4. 데이터 분석 프로젝트 목표 (모니터링 관점)
원활한 지하철 운행 모니터링을 위해 다음 4가지 분석을 우선순위로 합니다.

1) **배차 간격 정기성 분석 (Interval Regularity)**: 
   - 특정 역에 도착하는 열차들 사이의 시간 간격을 계산하여 배차가 밀리거나(Bunching) 너무 벌어지는 구간 탐지.
2) **지연 발생구간 탐지 (Delay Hotspots)**: 
   - 역별 체류 시간(`train_status` 1->2 변화 시간)을 측정하여 평시보다 오래 멈춰있는 역을 실시간 시각화.
3) **회차 효율성 분석 (Turnaround Efficiency)**: 
   - 종착역에 도착한 열차가 다시 반대 방향으로 투입되기까지의 소요 시간을 분석하여 회차 병목 구간 확인.
4) **급행/일반 열차 간섭 분석 (Congestion/Overtake)**: 
   - 급행 열차가 일반 열차를 추월하거나 앞선 열차 때문에 서행하는 구간을 탐지하여 운행 최적화 지점 도출.

## 5. 필수 요청사항
- 모든 코드는 OOP(객체지향) 또는 모듈화된 함수 형태로 작성한다.
- 예외 처리(Error Handling)를 포함하여 API 호출 실패나 DB 연결 오류 시 로그를 남겨야 한다.
- 한글 주석을 상세히 달아 팀원 간 코드 가독성을 높인다.