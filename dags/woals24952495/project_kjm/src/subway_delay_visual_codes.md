# 📈 지하철 지연 분석 시각화 상세 가이드 (Redash용 SQL 포함)

이 문서는 `table_redash_history`를 활용하여 레드애쉬(Redash)에서 바로 사용할 수 있는 쿼리와 시각화 설정 방법을 다룹니다.

---

## 💡 분석 1: 특정 역의 시간대별 지연 분포 (산점도)
**목표**: 특정 역에서 언제, 어떤 열차가 가장 많이 지연되는지 한눈에 파악합니다.

### 🔍 SQL 쿼리
```sql
SELECT 
    -- 1. X축: 예정 시간
    scheduled_arrival_text, 
    -- 2. Y축: 지연 시간 (분 단위 변환)
    ROUND((EXTRACT(EPOCH FROM delay_duration) / 60)::numeric, 1) AS delay_minutes,
    -- 3. 구분값 (이름표): 열차 번호 및 행선지
    train_code_num,
    dest_station_name
FROM table_redash_history
WHERE station_name = '{{ station_name }}'     -- 필터 1: 역 이름 (예: 서울역)
  AND line_name = '{{ line_name }}'           -- 필터 2: 호선 (예: 1호선)
  AND up_down = '{{ direction_0_or_1 }}'      -- 필터 3: 방향 (0:상행/내선, 1:하행/외선)
  AND created_date = CURRENT_DATE             -- 오늘 데이터 기준
  AND delay_duration > interval '0 seconds'    -- 지연된 데이터만
ORDER BY scheduled_arrival_text;
```

### 🎨 Redash 시각화 설정 (Scatter Plot)
1. **Visualization Type**: `Chart`
2. **Chart Type**: `Scatter`
3. **X Column**: `scheduled_arrival_text`
4. **Y Columns**: `delay_minutes`
5. **Name (Legend)**: `train_code_num` 또는 `dest_station_name` (데이터 포인트의 정보를 식별하기 위함)

---

## 💡 분석 2: 호선별 지연 '최고 기록' 역 랭킹 (Top 5)
**목표**: 오늘 하루 특정 호선에서 가장 극심한 지연이 발생한 역 5곳을 뽑습니다.

### 🔍 SQL 쿼리
```sql
SELECT 
    station_name,
    MAX(EXTRACT(EPOCH FROM delay_duration) / 60) AS max_delay_min,
    COUNT(*) AS delay_count -- 지연 발생 횟수
FROM table_redash_history
WHERE line_name = '{{ line_name }}'  -- 레드애쉬 파라미터 (예: 2호선)
  AND created_date = CURRENT_DATE
  AND delay_duration > interval '1 minute' -- 1분 이상 지연된 것만
GROUP BY station_name
ORDER BY max_delay_min DESC
LIMIT 5;
```

### 🎨 Redash 시각화 설정 (Bar Chart)
1. **Visualization Type**: `Chart`
2. **Chart Type**: `Bar` (Horizontal 추천)
3. **X Column**: `station_name`
4. **Y Columns**: `max_delay_min`

---

## 💡 분석 3: 출근 시간대(08-09시) 역별 지연 밀집도 (Bubble Chart)
**목표**: 지연 시간이 길고(+Y축), 지연 빈도가 잦은(+점의 크기) 역을 찾아냅니다.

### 🔍 SQL 쿼리
```sql
SELECT 
    station_name,
    AVG(EXTRACT(EPOCH FROM delay_duration) / 60) AS avg_delay,
    COUNT(*) AS delay_frequency
FROM table_redash_history
WHERE created_date = CURRENT_DATE
  AND kst_arrival::time BETWEEN '08:00:00' AND '09:30:00'
  AND delay_duration > interval '0 seconds'
GROUP BY station_name
HAVING COUNT(*) > 5 -- 최소 5번 이상 데이터가 찍힌 역만
ORDER BY avg_delay DESC;
```

### 🎨 Redash 시각화 설정 (Scatter -> Bubble)
1. **Visualization Type**: `Chart`
2. **Chart Type**: `Scatter`
3. **X Column**: `station_name`
4. **Y Columns**: `avg_delay`
5. **Bubble Size Column**: `delay_frequency` (지연이 잦은 역의 점이 커집니다)

---

## 💡 분석 4: 실시간 지연 변동성 "가장 심한 역"
**목표**: 단순히 많이 늦는 게 아니라, 시간표를 가장 안 지키는(변동이 심한) 역을 찾습니다.

### 🔍 SQL 쿼리
```sql
SELECT 
    station_name,
    STDDEV(EXTRACT(EPOCH FROM delay_duration) / 60) AS delay_volatility,
    AVG(EXTRACT(EPOCH FROM delay_duration) / 60) AS avg_delay
FROM table_redash_history
WHERE created_date = CURRENT_DATE
GROUP BY station_name
ORDER BY delay_volatility DESC
LIMIT 10;
```
*(기대 효과: 변동성이 크다는 것은 배차 간격이 일정하지 않다는 의미이므로 승객이 느끼는 피로도가 가장 높은 역을 골라낼 수 있습니다.)*
