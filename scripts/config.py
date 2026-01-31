# scripts/config.py
"""
YouTube 채널 분석기 공유 설정
"""
import os
from datetime import datetime, timezone

# Google Sheets
SHEET_NAME = '유튜브보물창고_테스트'
DATA_TAB_NAME = '데이터'
API_TAB_NAME = 'API_키_관리'

# 데이터 파일
DATA_DIR = 'data'
CHANNEL_IDS_FILE = os.path.join(DATA_DIR, 'channel_ids.json')
YOUTUBE_DATA_FILE = os.path.join(DATA_DIR, 'youtube_data.json')

# 컬럼 인덱스 (0-based)
HEADERS = [
    '채널명', 'URL', '핸들', '국가', '분류1', '분류2', '메모',
    '구독자', '동영상', '조회수', '최초업로드', '최근 업로드', '수집일',
    '최근 5개 토탈', '최근 10개 토탈', '최근 20개 토탈', '최근 30개 토탈',
    '키워드', '비고', '운영기간', '템플릿', '5일 기준', '10일 기준',
    'channel_id ', '5일조회수합계', '10일조회수합계', '15일조회수합계',
    'YT카테고리', '영상1', '영상2', '영상3', '영상4', '영상5'
]

COL_CHANNEL_NAME = 0
COL_URL = 1
COL_HANDLE = 2
COL_CHANNEL_ID = 23

# 국가 매핑
COUNTRY_MAP = {
    'KR': '한국', 'JP': '일본', 'US': '미국', 'GB': '영국',
    'DE': '독일', 'FR': '프랑스', 'VN': '베트남', 'TH': '태국',
    'ID': '인도네시아', 'IN': '인도', 'BR': '브라질', 'MX': '멕시코',
    'CA': '캐나다', 'AU': '호주', 'RU': '러시아', 'TR': '터키',
    'ES': '스페인', 'IT': '이탈리아', 'TW': '대만', 'HK': '홍콩', 'PH': '필리핀'
}

def get_now_utc():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

def get_data_dir():
    return os.path.abspath(DATA_DIR)

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

if __name__ == '__main__':
    print("✅ config.py 로드 성공!")
    print(f"SHEET_NAME: {SHEET_NAME}")
    print(f"DATA_DIR: {get_data_dir()}")
