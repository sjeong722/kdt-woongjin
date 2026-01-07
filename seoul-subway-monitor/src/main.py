import time
import schedule
import logging
from config import TARGET_LINES, SEOUL_API_KEY, SUPABASE_URL, SUPABASE_KEY
from api_client import SeoulSubwayAPI
from db_client import SubwayDBClient

# 로그 설정
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class SubwayMonitorApp:
    """
    지하철 데이터 수집 및 적재를 총괄하는 메인 애플리케이션 클래스
    """
    def __init__(self):
        self.api = SeoulSubwayAPI(SEOUL_API_KEY)
        self.db = SubwayDBClient(SUPABASE_URL, SUPABASE_KEY)

    def run_ingestion_job(self):
        """
        설정된 모든 호선에 대해 실시간 데이터를 수집하고 DB에 저장합니다.
        """
        logging.info("데이터 수집 작업을 시작합니다...")
        total_records = 0
        
        for line in TARGET_LINES:
            logging.info(f"{line} 데이터 수집 중...")
            data = self.api.fetch_realtime_position(line)
            
            if data:
                self.db.insert_data(data)
                total_records += len(data)
            else:
                logging.info(f"{line}에 대한 새로운 데이터가 없거나 수집에 실패했습니다.")
                
        logging.info(f"작업 완료. 총 {total_records}개의 레코드가 삽입되었습니다.")

def main():
    logging.info("서울 지하철 모니터링 시스템이 실행되었습니다.")
    
    app = SubwayMonitorApp()
    
    # 즉시 1회 실행
    app.run_ingestion_job()
    
    # 1분마다 주기적으로 실행하도록 스케줄 설정
    schedule.every(1).minutes.do(app.run_ingestion_job)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("사용자에 의해 시스템이 종료되었습니다.")

if __name__ == "__main__":
    main()

