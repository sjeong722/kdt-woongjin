import pandas as pd
import logging
from db_client import SubwayDBClient
from config import SUPABASE_URL, SUPABASE_KEY

# 로그 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SubwayAnalyzer:
    """
    수집된 지하철 위치 데이터를 분석하는 클래스
    """
    def __init__(self):
        self.db_client = SubwayDBClient(SUPABASE_URL, SUPABASE_KEY)
        self.supabase = self.db_client.supabase

    def fetch_data_from_db(self, limit=1000):
        """
        DB에서 최근 데이터를 가져와 Pandas DataFrame으로 변환합니다.
        """
        if not self.supabase:
            logging.error("DB 연결이 설정되지 않았습니다.")
            return None
        
        try:
            # 최근 1000개 데이터 조회
            response = self.supabase.table("realtime_subway_positions") \
                .select("*") \
                .order("created_at", descending=True) \
                .limit(limit) \
                .execute()
            
            if response.data:
                return pd.DataFrame(response.data)
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"데이터 조회 중 오류 발생: {e}")
            return None

    def analyze_interval_regularity(self, df):
        """
        1) 배차 간격 정기성 분석 (Interval Regularity)
        특정 역에 도착하는 열차들 사이의 시간 간격을 계산합니다.
        """
        if df.empty:
            return "분석할 데이터가 없습니다."

        # 도달 시간 형식 변환
        df['created_at'] = pd.to_datetime(df['created_at'])
        
        # 호선별/방향별 그룹화하여 시간 간격 계산
        results = []
        for (line, direction), group in df.groupby(['line_name', 'direction_type']):
            group = group.sort_values('created_at')
            group['interval'] = group['created_at'].diff().dt.total_seconds() / 60.0
            avg_interval = group['interval'].mean()
            results.append({
                "line": line,
                "direction": "상행/내선" if direction == 0 else "하행/외선",
                "avg_interval_min": round(avg_interval, 2)
            })
            
        return pd.DataFrame(results)

    def detect_delay_hotspots(self, df):
        """
        2) 지연 발생 구간 탐지 (Delay Hotspots)
        역별 체류 시간을 기반으로 지연 의심 역을 찾아냅니다.
        """
        # 이 부분은 동일 열차(train_number)의 상태 변화를 추적해야 하므로
        # 더 많은 시계열 데이터가 필요합니다.
        pass

if __name__ == "__main__":
    analyzer = SubwayAnalyzer()
    logging.info("데이터 분석을 시작합니다...")
    
    data = analyzer.fetch_data_from_db()
    if data is not None and not data.empty:
        intervals = analyzer.analyze_interval_regularity(data)
        print("=== 배차 간격 분석 결과 ===")
        print(intervals)
    else:
        logging.info("분석할 데이터가 DB에 없습니다. 먼저 main.py를 실행하여 데이터를 수집하세요.")
