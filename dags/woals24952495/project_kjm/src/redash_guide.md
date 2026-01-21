# 📊 레드애쉬(Redash) 연동 및 시각화 가이드

강사님 서버를 통해 Redash에 접속하고, 우리가 수집한 Supabase 데이터를 연결하여 시각화하는 방법을 단계별로 설명합니다.

---

## 1. Redash 접속 및 로그인
1.  **URL 접속**: 강사님께서 제공해주신 Redash 서버 주소로 접속합니다.
2.  **로그인**: 제공받은 ID와 비밀번호로 로그인합니다.

---

## 2. 데이터 소스(Data Source) 연결 (Supabase 연결)
Redash가 Supabase의 PostgreSQL 서버를 읽을 수 있도록 설정해야 합니다.

1.  우측 상단 설정 아이콘(⚙️) 클릭 -> **Data Sources** 선택.
2.  **+ New Data Source** 버튼 클릭.
3.  **PostgreSQL** 검색 후 선택.
4.  **연결 정보 입력** (Supabase 프로젝트의 `Settings -> Database` 정보 참고):
    *   **Name**: `Subway_Supabase` (자유롭게 지정)
    *   **Host**: Supabase에서 준 Host 주소
    *   **Port**: `5432`
    *   **User**: `postgres`
    *   **Password**: Supabase 비밀번호
    *   **Database Name**: `postgres`
5.  **Test Connection** 버튼을 눌러 "Success"가 뜨는지 확인 후 **Create** 클릭.

---

## 3. 쿼리 작성 (Create Query)
데이터를 가져오는 SQL 문을 작성합니다.

1.  상단 메뉴에서 **Create -> Query** 클릭.
2.  좌측에서 방금 만든 `Subway_Supabase` 선택.
3.  `visualization_plan.md`에 있는 샘플 쿼리 중 하나를 복사해서 붙여넣기.
    *   예: `SELECT * FROM table_redash_history WHERE created_date = CURRENT_DATE LIMIT 100;`
4.  **Execute** 버튼을 눌러 데이터가 하단에 잘 표출되는지 확인.
5.  **Save** 버튼을 눌러 쿼리 저장 (예: "오늘의 역별 지연 현황").

---

## 4. 시각화 구현 (Add Visualization)
표 형태의 데이터를 차트로 만듭니다.

1.  쿼리 결과창 바로 위 **+ Add Visualization** 버튼 클릭.
2.  **Visualization Type**: 원하는 차트 선택 (Scatter, Chart, Heatmap 등).
3.  **X Column** 및 **Y Columns**: 쿼리에서 가져온 필드 중 알맞은 것 선택.
4.  **Scatter Plot(산점도)** 예시:
    *   **X Column**: `kst_arrival` (시간)
    *   **Y Column**: `delay_duration_numeric` (지연 시간)
5.  설정 완료 후 **Save** 클릭.

---

## 5. 대시보드 구성 (Create Dashboard)
만든 시각화 도구들을 모아서 한 화면에 배치합니다.

1.  **Create -> Dashboard** 클릭.
2.  **Add Widget** 버튼 클릭 -> 작성한 쿼리 및 시각화 선택.
3.  마우스로 크기 및 위치 조절 후 **Publish** 클릭.

---

> [!TIP]
> **실시간 자동 갱신**: Redash 쿼리 설정에서 `Refresh Schedule`을 설정하면 (예: 10분마다), 에어플로우가 DB를 업데이트할 때 대시보드도 자동으로 최신 상태로 바뀝니다!
