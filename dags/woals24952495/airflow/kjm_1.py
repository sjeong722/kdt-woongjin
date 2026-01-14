import logging
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# [설정] 최신 API 키 및 타겟 노선
SEOUL_API_KEY = "6e71466270636b733633654f6b4a7a"
TARGET_LINES = ["1호선", "2호선", "3호선", "4호선", "5호선", "6호선", "7호선", "8호선", "9호선"]

default_args = dict(
    owner = 'woals24952495',
    retries = 1,
    retry_delay = pendulum.duration(seconds=30)
) 

with DAG(
    dag_id="woals24952495_subway_test_1815", # 기존 ID 유지
    start_date=pendulum.today('Asia/Seoul').add(days=-1),
    schedule="* * * * *",  # 테스트를 위해 매분 실행으로 변경
    catchup=False,
    default_args=default_args,
    tags=['subway', 'project', 'split_task'],
) as dag:

    # --- [STEP 1] API 데이터 수집 및 원천 저장 ---
    @task(task_id='1_fetch_and_load_raw')
    def fetch_and_load_raw():
        hook = PostgresHook(postgres_conn_id='jaemin1077_supabase_conn')
        conn = hook.get_sqlalchemy_engine()
        all_records = []
        
        for line in TARGET_LINES:
            try:
                url = f"http://swopenapi.seoul.go.kr/api/subway/{SEOUL_API_KEY}/json/realtimePosition/1/100/{line}"
                res = requests.get(url)
                data = res.json()
                if 'realtimePositionList' in data:
                    items = data['realtimePositionList']
                    for item in items:
                        all_records.append({
                            "line_name": item.get("subwayNm"),
                            "station_name": item.get("statnNm"),
                            "up_down": int(item.get("updnLine")) if str(item.get("updnLine")).isdigit() else 0,
                            "is_express": int(item.get("directAt")) if str(item.get("directAt")).isdigit() else 0,
                            "train_code": item.get("trainNo"),
                            "train_status": int(item.get("trainSttus")) if str(item.get("trainSttus")).isdigit() else 0,
                            "last_rec_time": pendulum.parse(item.get("recptnDt"), tz='Asia/Seoul') if item.get("recptnDt") else None,
                            "dest_station_name": item.get("statnTnm")
                        })
            except Exception as e:
                logging.error(f"API Error ({line}): {e}")

        if all_records:
            import pandas as pd
            df = pd.DataFrame(all_records)
            df.to_sql('_internal_raw_history', con=conn, if_exists='append', index=False)
            logging.info(f"Loaded {len(all_records)} records to DB.")
            return True
        else:
            logging.warning("No data found from API.")
            return False

    # --- [STEP 2] 데이터 가공 및 분석 테이블 생성 ---
    @task(task_id='2_transform_to_analysis_table')
    def transform_to_analysis_table(has_data):
        hook = PostgresHook(postgres_conn_id='jaemin1077_supabase_conn')
        
        # 오늘 날짜 기준 (Asia/Seoul)으로만 가공하여 '이전 프레임' 중복 방지
        sql = """
        CREATE TABLE IF NOT EXISTS _internal_raw_history (
            line_name TEXT, station_name TEXT, up_down INTEGER,
            is_express INTEGER, train_code TEXT, train_status INTEGER,
            last_rec_time TIMESTAMP, dest_station_name TEXT
        );

        DROP TABLE IF EXISTS final_realtime_subway;
        DROP TABLE IF EXISTS test1_realtime_summary_table;
        DROP TABLE IF EXISTS Subway_Arrival_Analysis;

        DROP TABLE IF EXISTS "00_FINAL_SUBWAY_DELAY_DATA";
        CREATE TABLE "00_FINAL_SUBWAY_DELAY_DATA" AS
        SELECT 
            DATE(last_rec_time) as created_date, 
            line_name,
            station_name,
            CASE WHEN up_down = 0 THEN '상행/내선' ELSE '하행/외선' END as direction,
            regexp_replace(train_code, '[^0-9]', '', 'g') as train_code_num,
            is_express,
            dest_station_name,
            MIN(last_rec_time) FILTER (WHERE train_status IN (0, 1)) as actual_arrival,
            MIN(last_rec_time) FILTER (WHERE train_status = 2) as actual_departure
        FROM _internal_raw_history
        WHERE DATE(last_rec_time) = (now() AT TIME ZONE 'Asia/Seoul')::date
        GROUP BY 1, 2, 3, 4, 5, 6, 7
        ORDER BY actual_arrival DESC;
        """
        hook.run(sql)
        logging.info("00_FINAL_SUBWAY_DELAY_DATA Table Refreshed.")

    # 작업 순서: 수집 완료 후 가공 실행
    data_loaded = fetch_and_load_raw()
    transform_to_analysis_table(data_loaded)