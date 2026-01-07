import logging
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_KEY

class SubwayDBClient:
    """
    Supabase (PostgreSQL)와 연동하여 지하철 데이터를 저장하고 관리하는 클래스
    """
    def __init__(self, url: str, key: str):
        self.url = url
        self.key = key
        self.supabase: Client = None
        
        if url and key:
            try:
                self.supabase = create_client(url, key)
            except Exception as e:
                logging.error(f"Supabase 클라이언트 초기화 실패: {e}")
        else:
            logging.error("Supabase URL 또는 Key가 누락되었습니다.")

    def map_api_to_db(self, item: dict) -> dict:
        """
        API 응답 데이터를 데이터베이스 스키마에 맞게 변환합니다.
        
        Args:
            item (dict): API로부터 받은 원본 데이터 객체
            
        Returns:
            dict: snake_case로 변환된 DB 적재용 객체
        """
        return {
            "line_id": item.get("subwayId"),
            "line_name": item.get("subwayNm"),
            "station_id": item.get("statnId"),
            "station_name": item.get("statnNm"),
            "train_number": item.get("trainNo"),
            "last_rec_date": item.get("lastRecptnDt"),
            "last_rec_time": item.get("recptnDt"),
            "direction_type": int(item.get("updnLine")) if item.get("updnLine") and str(item.get("updnLine")).isdigit() else None,
            "dest_station_id": item.get("statnTid"),
            "dest_station_name": item.get("statnTnm"),
            "train_status": int(item.get("trainSttus")) if item.get("trainSttus") and str(item.get("trainSttus")).isdigit() else None,
            "is_express": int(item.get("directAt")) if item.get("directAt") and str(item.get("directAt")).isdigit() else 0,
            "is_last_train": item.get("lstcarAt") == "1" or item.get("lstcarAt") == 1
        }

    def insert_data(self, data_list: list):
        """
        변환된 데이터 리스트를 Supabase 테이블에 일괄 삽입(Bulk Insert)합니다.
        
        Args:
            data_list (list): API 원본 데이터 리스트
        """
        if not self.supabase:
            logging.error("Supabase 클라이언트가 설정되지 않아 데이터를 삽입할 수 없습니다.")
            return

        if not data_list:
            return

        # 원본 데이터를 DB 형식으로 매핑
        records = [self.map_api_to_db(item) for item in data_list]
        
        try:
            # 'realtime_subway_positions' 테이블에 데이터 삽입
            response = self.supabase.table("realtime_subway_positions").insert(records).execute()
            logging.info(f"성공적으로 {len(records)}개의 레코드를 삽입했습니다.")
            return response
        except Exception as e:
            logging.error(f"Supabase 데이터 삽입 중 오류 발생: {e}")
            return None

