# scripts/config.py
"""
YouTube 채널 분석기 - 공유 설정 파일
모든 step에서 이 파일을 import해서 사용합니다.
"""

import os
from datetime import datetime

# ============================================================================
# 1️⃣ Google Sheets 설정
# ============================================================================
SHEET_NAME = os.getenv('SHEET_NAME', '유튜브보물창고_테스트')
DATA_TAB_NAME = os.getenv('DATA_TAB_NAME', '데이터')
API_TAB_NAME = os.getenv('API_TAB_NAME', 'API_키_관리')

# ============================================================================
# 2️⃣ 배치 처리 설정
# ============================================================================
BATCH_SIZE = 20  # 한 번에 20행씩 처리
SLEEP_BETWEEN_BATCHES = 2  # 배치 사이 2초 대기

# ============================================================================
# 3️⃣ API 설정
# ============================================================================
MAX_RETRIES = 3  # 최대 재시도 횟수
RETRY_DELAY = 2  # 재시도 대기 시간 (초)
RATE_LIMIT_WAIT = 60  # Rate Limit 시 대기 시간 (초)

# ============================================================================
# 4️⃣ Google Sheets 컬럼 (헤더명 기준)
# ============================================================================
# 실제 시트의 헤더명:
# ['채널명', 'URL', '핸들', '국가', '분류1', '분류2', '메모', '구독자', 
#  '동영상', '조회수', '최초업로드', '최근 업로드', '수집일', '최근 5개 토탈', 
#  '최근 10개 토탈', '최근 20개 토탈', '최근 30개 토탈', '키워드', '비고', '운영기간', 
#  '템플릿', '5일 기준', '10일 기준', 'channel_id ', '5일조회수합계', 
#  '10일조회수합계', '15일조회수합계', 'YT카테고리', '영상1', '영상2', '영상3', '영상4', '영상5']

# 컬럼명 → 인덱스 매핑 (0-based)
HEADERS = [
    '채널명',           # 0 (A)
    'URL',              # 1 (B)
    '핸들',             # 2 (C)
    '국가',             # 3 (D)
    '분류1',            # 4 (E)
    '분류2',            # 5 (F)
    '메모',             # 6 (G)
    '구독자',           # 7 (H)
    '동영상',           # 8 (I)
    '조회수',           # 9 (J)
    '최초업로드',       # 10 (K)
    '최근 업로드',      # 11 (L)
    '수집일',           # 12 (M)
    '최근 5개 토탈',    # 13 (N)
    '최근 10개 토탈',   # 14 (O)
    '최근 20개 토탈',   # 15 (P)
    '최근 30개 토탈',   # 16 (Q)
    '키워드',           # 17 (R)
    '비고',             # 18 (S)
    '운영기간',         # 19 (T)
    '템플릿',           # 20 (U)
    '5일 기준',         # 21 (V)
    '10일 기준',        # 22 (W)
    'channel_id ',      # 23 (X) - 주의: 스페이스 있음
    '5일조회수합계',    # 24 (Y)
    '10일조회수합계',   # 25 (Z)
    '15일조회수합계',   # 26 (AA)
    'YT카테고리',       # 27 (AB)
    '영상1',            # 28 (AC)
    '영상2',            # 29 (AD)
    '영상3',            # 30 (AE)
    '영상4',            # 31 (AF)
    '영상5',            # 32 (AG)
]

# 함수: 헤더명으로 인덱스 찾기
def get_column_index(header_name):
    """헤더명에 해당하는 인덱스 반환 (0-based)"""
    try:
        return HEADERS.index(header_name)
    except ValueError:
        print(f"⚠️  헤더 '{header_name}'을(를) 찾을 수 없습니다.")
        return None

# 자주 사용되는 컬럼들을 미리 정의
COL_CHANNEL_NAME = get_column_index('채널명')          # 0
COL_URL = get_column_index('URL')                      # 1
COL_HANDLE = get_column_index('핸들')                  # 2
COL_COUNTRY = get_column_index('국가')                 # 3
COL_CATEGORY_1 = get_column_index('분류1')             # 4
COL_CATEGORY_2 = get_column_index('분류2')             # 5
COL_MEMO = get_column_index('메모')                    # 6
COL_SUBSCRIBERS = get_column_index('구독자')           # 7
COL_VIDEO_COUNT = get_column_index('동영상')           # 8
COL_TOTAL_VIEWS = get_column_index('조회수')           # 9
COL_FIRST_UPLOAD = get_column_index('최초업로드')      # 10
COL_LATEST_UPLOAD = get_column_index('최근 업로드')    # 11
COL_COLLECT_DATE = get_column_index('수집일')          # 12
COL_VIEWS_5_TOTAL = get_column_index('최근 5개 토탈')  # 13
COL_VIEWS_10_TOTAL = get_column_index('최근 10개 토탈') # 14
COL_VIEWS_20_TOTAL = get_column_index('최근 20개 토탈') # 15
COL_VIEWS_30_TOTAL = get_column_index('최근 30개 토탈') # 16
COL_KEYWORD = get_column_index('키워드')               # 17
COL_NOTE = get_column_index('비고')                    # 18
COL_OPERATION_DAYS = get_column_index('운영기간')      # 19
COL_TEMPLATE = get_column_index('템플릿')              # 20
COL_COUNT_5D = get_column_index('5일 기준')            # 21
COL_COUNT_10D = get_column_index('10일 기준')          # 22
COL_CHANNEL_ID = get_column_index('channel_id ')       # 23 (스페이스 주의!)
COL_VIEWS_5D = get_column_index('5일조회수합계')       # 24
COL_VIEWS_10D = get_column_index('10일조회수합계')     # 25
COL_VIEWS_15D = get_column_index('15일조회수합계')     # 26
COL_YT_CATEGORY = get_column_index('YT카테고리')       # 27
COL_VIDEO_LINKS = [
    get_column_index('영상1'),                         # 28
    get_column_index('영상2'),                         # 29
    get_column_index('영상3'),                         # 30
    get_column_index('영상4'),                         # 31
    get_column_index('영상5'),                         # 32
]

# 수동 입력 컬럼 (덮어쓰지 않음)
MANUAL_INPUT_COLUMNS = [
    COL_CATEGORY_1, 
    COL_CATEGORY_2, 
    COL_MEMO, 
    COL_KEYWORD, 
    COL_NOTE, 
    COL_TEMPLATE
]

# ============================================================================
# 5️⃣ 데이터 파일 경로 (step 간 데이터 전달용)
# ============================================================================
DATA_DIR = 'data'  # data 폴더
CHANNEL_IDS_FILE = f'{DATA_DIR}/channel_ids.json'      # Step 1 → Step 2
YOUTUBE_DATA_FILE = f'{DATA_DIR}/youtube_data.json'    # Step 2 → Step 3

# ============================================================================
# 6️⃣ 한국어 매핑
# ============================================================================
COUNTRY_MAP = {
    'KR': '한국',
    'JP': '일본',
    'US': '미국',
    'GB': '영국',
    'DE': '독일',
    'FR': '프랑스',
    'VN': '베트남',
    'TH': '태국',
    'ID': '인도네시아',
    'IN': '인도',
    'BR': '브라질',
    'MX': '멕시코',
    'CA': '캐나다',
    'AU': '호주',
    'RU': '러시아',
    'TR': '터키',
    'ES': '스페인',
    'IT': '이탈리아',
    'TW': '대만',
    'HK': '홍콩',
    'PH': '필리핀',
}

CATEGORY_MAP = {
    '10': '음악',
    '17': '스포츠',
    '18': '쇼',
    '19': '트레일러',
    '20': '동영상',
    '21': '영화',
    '22': '애니메이션',
    '23': '뉴스',
    '24': '쇼',
    '25': '과학 및 기술',
    '26': '다큐멘터리',
    '27': '영화',
    '28': '액션/어드벤처',
    '29': '클래식',
    '30': '코미디',
    '31': '범죄',
    '32': '다큐멘터리',
    '33': '드라마',
    '34': '가족',
    '35': '외국 영화',
    '36': '호러',
    '37': 'SF',
    '38': '스릴러',
    '39': '단편',
    '40': '쇼',
    '41': '서스펜스',
    '42': '영화',
    '43': '영화',
    '44': '영화',
}

# ============================================================================
# 7️⃣ 유틸리티 함수
# ============================================================================
def get_now_utc():
    """현재 시간 (UTC)"""
    return datetime.utcnow()

def get_data_dir():
    """data 폴더 생성 (없으면)"""
    os.makedirs(DATA_DIR, exist_ok=True)
    return DATA_DIR

def get_header_by_index(index):
    """인덱스로 헤더명 가져오기"""
    if 0 <= index < len(HEADERS):
        return HEADERS[index]
    return None

# ============================================================================
# 8️⃣ 로깅 설정
# ============================================================================
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
