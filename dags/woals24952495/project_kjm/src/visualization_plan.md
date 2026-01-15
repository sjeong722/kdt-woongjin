# 📊 서울 지하철 지연 분석 시각화 기획서

이 문서는 Supabase에 수집된 `table_redash_history` 데이터를 활용하여 Redash 대시보드에서 구현할 시각화 아이디어와 구체적인 설정 방법을 정리합니다.

---

## 💡 아이디어 A: 역별 지연 변동성 산점도 (Scatter Plot)
특정 역을 통과하는 모든 열차의 지연 시간을 점으로 찍어, 특정 시간대에 지연이 집중되는지 확인합니다.

### 🔍 SQL 쿼리 (특정 역 기준)
```sql
SELECT 
    scheduled_arrival_text,
    -- 지연 시간을 '분' 단위 숫자로 변환 (Redash 산점도용)
    ROUND((EXTRACT(EPOCH FROM (delay_duration)) / 60)::numeric, 1) AS delay_minutes,
    train_code_num,
    dest_station_name,
    CASE WHEN up_down = '0' THEN '상행/내선' ELSE '하행/외선' END AS direction
FROM table_redash_history
WHERE station_name = '{{ station_name }}' -- 레드애쉬 입력창 생성
  AND created_date = CURRENT_DATE -- 오늘 데이터만 보기
  AND delay_duration > interval '0 seconds'
ORDER BY scheduled_arrival_text;
```

### 🎨 시각화 방법 (Redash 설정)
1. **Add Visualization** 클릭 후 **Visualization Type**을 `Chart`로 선택합니다.
2. **Chart Type**: `Scatter`를 선택합니다.
3. **X Column**: `scheduled_arrival_text`를 선택합니다.
4. **Y Columns**: `delay_minutes`를 선택합니다.
5. **Group by**: `direction`을 선택하여 상/하행에 따라 점의 색깔을 다르게 표시합니다.
6. **Data Labels**: `train_code_num`이나 `dest_station_name`을 추가하여 마우스 오버 시 정보를 확인합니다.

---

## 💡 아이디어 B: 호선별 실시간 지연 랭킹 (Bar Chart)
현재 어느 호선이 가장 많이 밀리고 있는지 전체적인 지연 평균을 비교합니다.

### 🔍 SQL 쿼리 (호선별 평균)
```sql
SELECT 
    line_name,
    ROUND(AVG(EXTRACT(EPOCH FROM (delay_duration)) / 60)::numeric, 1) AS avg_delay_min,
    COUNT(*) AS total_cases
FROM table_redash_history
WHERE created_date = CURRENT_DATE
  AND delay_duration > interval '0 seconds'
GROUP BY line_name
ORDER BY avg_delay_min DESC;
```

### 🎨 시각화 방법 (Redash 설정)
1. **Add Visualization** 클릭 후 **Visualization Type**을 `Chart`로 선택합니다.
2. **Chart Type**: `Bar`를 선택합니다.
3. **X Column**: `line_name`을 선택합니다.
4. **Y Columns**: `avg_delay_min`을 선택합니다.
5. **Sort Values**: `Descending`으로 설정하여 가장 지연이 심한 호선이 왼쪽/위에 오게 합니다.
6. **Aesthetics**: 각 호선의 공식 색상에 맞춰 막대 색상을 지정하면 더욱 직관적입니다.

---

## 💡 아이디어 C: 시간대별 호선별 지연 히트맵 (Heatmap)
하루 종일 어느 시간대에 어느 호선이 붉게 물드는지(지연이 심한지) 흐름을 봅니다.

### 🔍 SQL 쿼리 (시간대별 집계)
```sql
SELECT 
    line_name,
    EXTRACT(HOUR FROM kst_arrival) AS arrival_hour,
    ROUND(AVG(EXTRACT(EPOCH FROM (delay_duration)) / 60)::numeric, 1) AS avg_delay_min
FROM table_redash_history
WHERE created_date = CURRENT_DATE
  AND delay_duration > interval '0 seconds'
GROUP BY 1, 2
ORDER BY 2, 1;
```

### 🎨 시각화 방법 (Redash 설정)
1. **Add Visualization** 클릭 후 **Visualization Type**을 `Pivot Table` 또는 `Chart (Heatmap)`을 선택합니다.
2. **X Column (Columns)**: `arrival_hour` (시각)를 선택합니다.
3. **Y Column (Rows)**: `line_name` (호선)을 선택합니다.
4. **Color Column (Values)**: `avg_delay_min`을 선택합니다.
5. **Color Scheme**: `Red-Yellow-Blue`나 `YlOrRd` (노랑-주황-빨강)을 선택하여 지연이 심할수록 빨갛게 보이도록 설정합니다.

---

## 🏛️ 대시보드 최종 레이아웃 제안
1. **상단 (Counter)**: "오늘의 전체 평균 지연 시간", "최대 지연 발생 건수" 등 주요 수치 카드.
2. **중간 (Heatmap)**: 호선별/시간대별 전체 흐름을 보는 히트맵.
3. **하단 좌측 (Bar Chart)**: 현재 지연이 가장 심한 호선 TOP 5.
4. **하단 우측 (Scatter Plot)**: 특정 관심 역(예: 환승역)의 개별 열차 지연 분포.
