import requests
import logging
from config import SEOUL_API_KEY

BASE_URL = "http://swopenapi.seoul.go.kr/api/subway"

class SeoulSubwayAPI:
    """
    서울시 실시간 지하철 위치 정보를 가져오는 API 클라이언트 클래스
    """
    def __init__(self, api_key: str):
        self.api_key = api_key
        if not self.api_key:
            logging.error("서울시 API 키가 설정되지 않았습니다.")

    def fetch_realtime_position(self, line_name: str, start_index: int = 1, end_index: int = 100):
        """
        특정 호선의 실시간 열차 위치 정보를 조회합니다.
        
        Args:
            line_name (str): 지하철 호선 명 (예: '1호선', '분당선')
            start_index (int): 페이징 시작 인덱스 (기본값: 1)
            end_index (int): 페이징 종료 인덱스 (기본값: 100)
            
        Returns:
            list: 실시간 위치 정보 리스트 (오류 발생 시 None 또는 빈 리스트)
        """
        if not self.api_key:
            return None

        # API 호출 URL 구성
        # 구성: http://swopenapi.seoul.go.kr/api/subway/(key)/json/realtimePosition/(start)/(end)/(subwayNm)
        url = f"{BASE_URL}/{self.api_key}/json/realtimePosition/{start_index}/{end_index}/{line_name}"
        
        try:
            response = requests.get(url)
            response.raise_for_status() # HTTP 오류 발생 시 예외 발생
            data = response.json()
            
            # API 응답 결과 확인
            if 'realtimePositionList' in data:
                return data['realtimePositionList']
            elif 'RESULT' in data:
                code = data['RESULT'].get('CODE')
                message = data['RESULT'].get('MESSAGE')
                if code != 'INFO-000':
                    logging.warning(f"{line_name} API 응답 오류: {code} - {message}")
                return []
            
            return []
            
        except requests.exceptions.RequestException as e:
            logging.error(f"{line_name} 데이터 수집 중 네트워크 오류 발생: {e}")
            return None
        except Exception as e:
            logging.error(f"{line_name} 데이터 처리 중 알 수 없는 오류 발생: {e}")
            return None

