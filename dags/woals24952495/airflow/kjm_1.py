import logging
import requests
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configuration
SEOUL_API_KEY = "6e71466270636b733633654f6b4a7a" # 최신 키 반영
TARGET_LINES = [
    "1호선", "2호선", "3호선", "4호선", "5호선",
    "6호선", "7호선", "8호선", "9호선",
    "경의중앙선", "공항철도", "수인분당선", "경춘선"
]
default_args = dict(
    owner = 'woals24952495',
    email = ['woals24952495@naver.com'],
    email_on_failure = False,
    retries = 1
) 

with DAG(
    dag_id="woals24952495_subway_test_1815", # 기존 ID 유지
    start_date=pendulum.today('Asia/Seoul').add(days=-1),
    schedule="22-29 19 * * *",  # 19:22 ~ 19:29 매분 실행 (오늘 마지막 테스트)
    catchup=False,
    default_args=default_args,
    tags=['subway', 'project', 'woals24952495'],
) as dag:

    @task(task_id='collect_and_insert_subway_data')
    def collect_and_insert_subway_data():
        hook = PostgresHook(postgres_conn_id='jaemin1077_supabase_conn')
        conn = hook.get_sqlalchemy_engine()
        all_records = []
        
        # 1. API 데이터 수집
        for line in TARGET_LINES:
            try:
                url = f"http://swopenapi.seoul.go.kr/api/subway/{SEOUL_API_KEY}/json/realtimePosition/1/100/{line}"
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                if 'realtimePositionList' in data:
                    items = data['realtimePositionList']
                    for item in items:
                        record = {
                            "line_id": item.get("subwayId"),
                            "line_name": item.get("subwayNm"),
                            "station_name": item.get("statnNm"),
                            "up_down": int(item.get("updnLine")) if item.get("updnLine") and str(item.get("updnLine")).isdigit() else None,
                            "is_express": int(item.get("directAt")) if item.get("directAt") and str(item.get("directAt")).isdigit() else 0,
                            "train_code": item.get("trainNo"),
                            "train_status": int(item.get("trainSttus")) if item.get("trainSttus") and str(item.get("trainSttus")).isdigit() else None,
                            "last_rec_time": pendulum.parse(item.get("recptnDt"), tz='Asia/Seoul') if item.get("recptnDt") else None,
                            "dest_station_id": item.get("statnTid"),
                            "dest_station_name": item.get("statnTnm")
                        }
                        all_records.append(record)
            except Exception as e:
                logging.error(f"Error fetching {line}: {e}")

        if all_records:
            # 2. 내부 히스토리 적재 (기존의 지저분한 테이블 대체)
            import pandas as pd
            df = pd.DataFrame(all_records)
            df.to_sql('_internal_raw_history', con=conn, if_exists='append', index=False)
            logging.info("Raw data appended.")

        # 3. [핵심] 가공된 '진짜' 테이블 생성
        # 사용자님이 원하신 컬럼 이름과 가공 로직만 적용된 물리 테이블입니다.
        try:
            logging.info("Refreshing final analysis table (00_FINAL_SUBWAY_DELAY_DATA)...")
            sql = """
            -- 구버전 테이블들 삭제
            DROP TABLE IF EXISTS final_realtime_subway;
            DROP TABLE IF EXISTS test1_realtime_summary_table;
            DROP TABLE IF EXISTS Subway_Arrival_Analysis;
            
            -- 최종 가공 테이블 생성
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
            WHERE train_status IN (0, 1, 2)
            GROUP BY 1, 2, 3, 4, 5, 6, 7
            ORDER BY actual_arrival DESC;
            """
            hook.run(sql)
            logging.info("Analysis table refresh completed!")
        except Exception as e:
            logging.error(f"SQL Error: {e}")

    collect_and_insert_subway_data()