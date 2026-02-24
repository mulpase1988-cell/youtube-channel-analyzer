# ========================================
# YouTube 채널 분석기 v2 - GitHub Actions 버전
# RSS + yt-dlp 하이브리드 방식 (API 호출 제거)
# ✅ 수정1: 영상 링크 → 썸네일 URL로 변경
# ✅ 수정2: 운영기간(T열) = L열(최근 업로드) - K열(최초 업로드)
# ✅ 수정3: 채널 썸네일 URL 추가 (AH열)
# ========================================

# ========================================
# 1. 라이브러리 임포트
# ========================================
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta
from dateutil import parser as dateutil_parser
import feedparser
import subprocess
import json
import time
import re
import urllib.parse
import traceback
import os
import tempfile
import random
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import atexit

# ========================================
# 2. 설정 변수
# ========================================

# 🔥 환경변수에서 인증 정보 로드
SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT')
if not SERVICE_ACCOUNT_JSON:
    raise Exception("❌ GOOGLE_SERVICE_ACCOUNT 환경변수가 설정되지 않았습니다")

# JSON을 임시 파일로 저장
with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
    f.write(SERVICE_ACCOUNT_JSON)
    SERVICE_ACCOUNT_FILE = f.name

# 임시 파일 정리 함수
def cleanup_temp_file():
    try:
        if SERVICE_ACCOUNT_FILE and os.path.exists(SERVICE_ACCOUNT_FILE):
            os.remove(SERVICE_ACCOUNT_FILE)
            print("✅ 임시 파일 정리 완료")
    except Exception as e:
        print(f"⚠️ 임시 파일 정리 실패: {e}")

atexit.register(cleanup_temp_file)

# Google Sheets 설정 (환경변수 우선, 없으면 기본값)
SHEET_NAME = os.environ.get('SHEET_NAME', '유튜브보물창고_테스트')
API_TAB_NAME = os.environ.get('API_TAB_NAME', 'API_키_관리')
DATA_TAB_NAME = os.environ.get('DATA_TAB_NAME', '데이터')

# 배치 업데이트 설정
BATCH_SIZE = 20  # 20행씩 배치 처리

# 컬럼 매핑 (A=1, B=2, ...)
COL_CHANNEL_NAME = 1      # A: 채널명
COL_URL = 2                # B: URL
COL_HANDLE = 3             # C: 핸들
COL_COUNTRY = 4            # D: 국가
COL_CATEGORY_1 = 5         # E: 분류1 (수동)
COL_CATEGORY_2 = 6         # F: 분류2 (수동)
COL_MEMO = 7               # G: 메모 (수동)
COL_SUBSCRIBERS = 8        # H: 구독자
COL_VIDEO_COUNT = 9        # I: 동영상
COL_TOTAL_VIEWS = 10       # J: 조회수
COL_FIRST_UPLOAD = 11      # K: 최초업로드
COL_LATEST_UPLOAD = 12     # L: 최근 업로드
COL_COLLECT_DATE = 13      # M: 수집일
COL_VIEWS_5_TOTAL = 14     # N: 최근 5개 토탈
COL_VIEWS_10_TOTAL = 15    # O: 최근 10개 토탈
COL_VIEWS_20_TOTAL = 16    # P: 최근 20개 토탈
COL_VIEWS_30_TOTAL = 17    # Q: 최근 30개 토탈
COL_KEYWORD = 18           # R: 키워드 (수동)
COL_NOTE = 19              # S: 비고 (수동)
COL_OPERATION_DAYS = 20    # T: 운영기간
COL_TEMPLATE = 21          # U: 템플릿 (수동)
COL_COUNT_5D = 22          # V: 5일 기준
COL_COUNT_10D = 23         # W: 10일 기준
COL_CHANNEL_ID = 24        # X: channel_id
COL_VIEWS_5D = 25          # Y: 5일조회수합계
COL_VIEWS_10D = 26         # Z: 10일조회수합계
COL_VIEWS_15D = 27         # AA: 15일조회수합계
COL_YT_CATEGORY = 28       # AB: YT카테고리
COL_VIDEO_LINKS = [29, 30, 31, 32, 33]  # AC~AG: 영상썸네일1~5
COL_CHANNEL_THUMBNAIL = 34  # AH: 채널 썸네일 ✅ 추가

# 수동 입력 컬럼
MANUAL_INPUT_COLUMNS = [COL_CATEGORY_1, COL_CATEGORY_2, COL_MEMO, 
                        COL_KEYWORD, COL_NOTE, COL_TEMPLATE]

# 국가 코드 → 한글 매핑
COUNTRY_MAP = {
    'KR': '한국', 'US': '미국', 'JP': '일본', 'GB': '영국', 
    'DE': '독일', 'FR': '프랑스', 'CA': '캐나다', 'AU': '호주',
    'VN': '베트남', 'TH': '태국', 'ID': '인도네시아', 'IN': '인도',
    'BR': '브라질', 'MX': '멕시코', 'RU': '러시아', 'TR': '터키',
    'ES': '스페인', 'IT': '이탈리아', 'TW': '대만', 'HK': '홍콩',
    'PH': '필리핀', 'CN': '중국', 'SG': '싱가포르', 'MY': '말레이시아'
}

# 카테고리 ID → 한글 매핑
CATEGORY_MAP = {
    '1': '영화/애니메이션', '2': '자동차/차량', '10': '음악',
    '15': '반려동물/동물', '17': '스포츠', '18': '단편 동영상',
    '19': '여행/이벤트', '20': '게임', '21': '브이로그',
    '22': '인물/블로그', '23': '코미디', '24': '엔터테인먼트',
    '25': '뉴스/정치', '26': '노하우/스타일', '27': '교육',
    '28': '과학기술', '29': '비영리/사회운동'
}

# ========================================
# 3. 헬퍼 함수들
# ========================================
def get_country_name(country_code):
    """국가 코드를 한글명으로 변환 (빈 값이면 '한국' 기본값)"""
    if not country_code or country_code.strip() == '':
        return '한국'
    return COUNTRY_MAP.get(country_code.upper(), country_code)

def get_category_name(category_id):
    """카테고리 ID를 한글명으로 변환"""
    if not category_id:
        return '미분류'
    return CATEGORY_MAP.get(str(category_id), '미분류')

def get_thumbnail_urls(video_infos, max_count=5):
    """✅ 수정: 상위 5개 영상의 썸네일 URL 리스트 반환 (고해상도 우선)"""
    urls = []
    for video_info in video_infos[:max_count]:
        try:
            # RSS 썸네일 직접 사용
            if video_info.get('thumbnail_url'):
                urls.append(video_info['thumbnail_url'])
            else:
                urls.append('')
        except Exception as e:
            print(f"  ⚠️  썸네일 추출 실패: {e}")
            urls.append('')
    
    # 부족한 칸 채우기
    while len(urls) < max_count:
        urls.append('')
    
    return urls

def parse_published_date(date_str):
    """다양한 형식의 날짜 문자열을 파싱"""
    if not date_str:
        return None
    try:
        dt = dateutil_parser.parse(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception as e:
        print(f"⚠️ 날짜 파싱 실패: {date_str} | {e}")
        return None

# ========================================
# 4. 재시도 데코레이터 (RSS 통신용)
# ========================================
MAX_RETRIES = 3
RETRY_DELAY = 2

def retry_with_backoff(func):
    """지수 백오프를 사용한 재시도 데코레이터"""
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    print(f"  ⚠️  일시적 오류: {str(e)[:50]}... {wait_time:.1f}초 대기 후 재시도 ({attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    print(f"  ⚠️  오류: {str(e)[:50]}... {wait_time:.1f}초 대기 후 재시도 ({attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
        
        raise Exception(f"❌ {MAX_RETRIES}회 재시도 후에도 실패")
    
    return wrapper

# ========================================
# 5. RSS 피드 파싱 (강화 버전)
# ========================================
@retry_with_backoff
def parse_rss_feed(channel_id, max_videos=15):
    """YouTube RSS 피드에서 최근 영상 정보 추출 (강화 버전)"""
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    try:
        response = session.get(rss_url, timeout=10)
        response.raise_for_status()
        
        feed = feedparser.parse(response.content)

        if not feed.entries:
            return []

        videos = []
        for entry in feed.entries[:max_videos]:
            try:
                video_id = entry.yt_videoid if hasattr(entry, 'yt_videoid') else None
                if not video_id and 'id' in entry:
                    video_id = entry.id.split(':')[-1]

                published_str = entry.published if hasattr(entry, 'published') else None
                published_at = parse_published_date(published_str)

                # 썸네일 URL 추출
                thumbnail_url = ''
                if hasattr(entry, 'media_content'):
                    for media in entry.media_content:
                        if media.get('type', '').startswith('image'):
                            thumbnail_url = media.get('url', '')
                            break

                videos.append({
                    'video_id': video_id,
                    'title': entry.title if hasattr(entry, 'title') else '',
                    'published_at': published_at,
                    'thumbnail_url': thumbnail_url
                })
            except Exception as e:
                print(f"  ⚠️  RSS 항목 파싱 실패: {e}")
                continue

        return videos

    except Exception as e:
        print(f"  ⚠️  RSS 피드 파싱 실패: {e}")
        return []

# ========================================
# 6. yt-dlp로 채널 정보 추출
# ========================================
def get_channel_info_ytdlp(channel_url):
    """yt-dlp로 채널 정보 추출 (YouTube API 대체)"""
    try:
        # URL 정규화
        if '@' in channel_url:
            handle = channel_url.split('@')[-1].split('/')[0]
            channel_url = f"https://www.youtube.com/@{handle}"
        elif '/channel/' not in channel_url and '/user/' not in channel_url:
            channel_url = channel_url if channel_url.startswith('http') else f"https://www.youtube.com{channel_url}"
        
        print(f"  🎬 yt-dlp로 채널 정보 추출 중...")
        
        result = subprocess.run(
            ['yt-dlp', '--dump-json', '--no-warnings', '-e', 'generic', channel_url],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            print(f"  ⚠️  yt-dlp 실행 실패")
            return None
        
        data = json.loads(result.stdout)
        
        channel_info = {
            'channel_id': data.get('channel_id', ''),
            'channel_name': data.get('uploader', '') or data.get('channel', ''),
            'handle': data.get('channel_url', '').split('@')[-1] if data.get('channel_url') else '',
            'thumbnail_url': data.get('thumbnail', ''),
        }
        
        print(f"  ✓ Channel ID: {channel_info['channel_id'][:20]}...")
        print(f"  ✓ Channel Name: {channel_info['channel_name']}")
        
        return channel_info
    
    except subprocess.TimeoutExpired:
        print(f"  ❌ yt-dlp 타임아웃 (30초)")
        return None
    except json.JSONDecodeError:
        print(f"  ❌ yt-dlp JSON 파싱 실패")
        return None
    except FileNotFoundError:
        print(f"  ❌ yt-dlp를 찾을 수 없습니다")
        return None
    except Exception as e:
        print(f"  ❌ yt-dlp 오류: {e}")
        return None

# ========================================
# 7. 채널 ID 추출
# ========================================
def extract_channel_id_from_url(channel_url, row_number, row_data=None):
    """채널 URL에서 channel_id 추출"""
    if '/channel/' in channel_url:
        return channel_url.split('/channel/')[-1].split('/')[0].split('?')[0]

    handle_from_sheet = None
    if row_data and len(row_data) >= COL_HANDLE:
        handle_from_sheet = str(row_data[COL_HANDLE - 1]).strip()

        if handle_from_sheet:
            if handle_from_sheet.startswith('@'):
                handle_from_sheet = handle_from_sheet[1:]

            try:
                handle_decoded = urllib.parse.unquote(handle_from_sheet)
                print(f"  📋 C열에서 핸들 사용: @{handle_decoded}")
                
                channel_url_for_handle = f"https://www.youtube.com/@{handle_decoded}"
                channel_info = get_channel_info_ytdlp(channel_url_for_handle)
                if channel_info and channel_info.get('channel_id'):
                    return channel_info['channel_id']
            except Exception as e:
                print(f"  ⚠️  C열 핸들로 추출 실패: {e}")

    decoded_url = urllib.parse.unquote(channel_url)
    handle_match = re.search(r'@([^/\s?]+)', decoded_url)
    
    if not handle_match:
        print(f"  ⚠️  URL에서 핸들을 추출할 수 없음")
        return None

    handle = handle_match.group(1)
    print(f"  📍 URL에서 핸들 추출: @{handle}")

    try:
        channel_url_for_handle = f"https://www.youtube.com/@{handle}"
        channel_info = get_channel_info_ytdlp(channel_url_for_handle)
        if channel_info and channel_info.get('channel_id'):
            return channel_info['channel_id']
    except Exception as e:
        print(f"  ⚠️  forHandle 실패: {e}")

    return None

def extract_channel_id_ytdlp(url):
    """yt-dlp로 채널 ID 추출 (백업)"""
    try:
        result = subprocess.run(
            ['yt-dlp', '--dump-json', '--no-warnings', '-e', 'generic', '--playlist-items', '1', url],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            return data.get('channel_id')
    except Exception as e:
        print(f"⚠️ yt-dlp 추출 실패: {e}")
    
    return None

# ========================================
# 8. Shorts 채널 데이터 수집 (yt-dlp 기반)
# ========================================
def get_shorts_channel_data(channel_id, row_number):
    """Shorts 전용 채널에서 영상 데이터 수집 (yt-dlp)"""
    shorts_videos = []
    
    print(f"  🎬 Shorts 채널 검사 중...")
    try:
        shorts_url = f"https://www.youtube.com/channel/{channel_id}/shorts"
        
        result = subprocess.run(
            ['yt-dlp', '--dump-json', '--no-warnings', '-e', 'generic',
             '--playlist-items', '1:30', shorts_url],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                try:
                    data = json.loads(line)
                    published_str = data.get('upload_date', '')
                    if published_str:
                        # YYYYMMDD 형식 → YYYY-MM-DD로 변환
                        if len(published_str) == 8:
                            published_str = f"{published_str[0:4]}-{published_str[4:6]}-{published_str[6:8]}"
                    
                    shorts_videos.append({
                        'video_id': data.get('id', ''),
                        'title': data.get('title', ''),
                        'published_at': parse_published_date(published_str),
                        'thumbnail_url': data.get('thumbnail', '')
                    })
                except:
                    pass
            
            if shorts_videos:
                print(f"  ✓ Shorts 채널 감지: {len(shorts_videos)}개 영상 수집")
            
            return shorts_videos
    
    except Exception as e:
        print(f"  ⚠️  Shorts 채널 조회 실패: {e}")
        return []

# ========================================
# 9. 메인 채널 데이터 수집 (API 없음)
# ========================================
def get_channel_data_hybrid(channel_url, row_number, row_data, worksheet):
    """RSS + yt-dlp 하이브리드 방식으로 채널 데이터 수집 (API 없음)"""
    result = {
        'channel_name': '',
        'handle': '',
        'country': '',
        'subscribers': 0,
        'video_count': 0,
        'total_views': 0,
        'first_upload': '',
        'latest_upload': '',
        'collect_date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
        'views_5': 0,
        'views_10': 0,
        'views_20': 0,
        'views_30': 0,
        'views_5d': 0,
        'views_10d': 0,
        'views_15d': 0,
        'count_5d': 0,
        'count_10d': 0,
        'operation_days': 0,
        'channel_id': '',
        'yt_category': '미분류',
        'channel_thumbnail': '',  # ✅ 추가
        'video_links': ['', '', '', '', '']
    }

    try:
        existing_channel_id = ''
        if len(row_data) >= COL_CHANNEL_ID:
            existing_channel_id = str(row_data[COL_CHANNEL_ID - 1]).strip()

        channel_id = existing_channel_id

        if not channel_id:
            print(f"  📍 channel_id 없음, 검색 필요...")
            channel_id = extract_channel_id_from_url(
                channel_url,
                row_number,
                row_data=row_data
            )

            if not channel_id:
                print(f"  ❌ channel_id 추출 실패, yt-dlp로 백업 시도")
                channel_id = extract_channel_id_ytdlp(channel_url)
                
                if not channel_id:
                    return None

            try:
                cell_list = [gspread.Cell(row_number, COL_CHANNEL_ID, channel_id)]
                worksheet.update_cells(cell_list)
                print(f"  ✅ channel_id 저장 완료: {channel_id}")
                time.sleep(2)
            except Exception as e:
                print(f"  ⚠️  channel_id 저장 실패: {e}")
        else:
            print(f"  ✓ 기존 channel_id 사용: {channel_id}")

        result['channel_id'] = channel_id

        # yt-dlp로 채널 정보 추출
        print(f"  🎬 yt-dlp로 채널 정보 추출 중...")
        channel_info = get_channel_info_ytdlp(channel_url)
        
        if channel_info:
            result['channel_name'] = channel_info.get('channel_name', '')
            result['handle'] = channel_info.get('handle', '')
            result['channel_thumbnail'] = channel_info.get('thumbnail_url', '')  # ✅ 추가
            print(f"  ✓ 채널: {result['channel_name']}")
            if result['channel_thumbnail']:
                print(f"  ✅ 채널 썸네일: {result['channel_thumbnail'][:80]}...")

        # RSS 피드 수집
        print(f"  📡 RSS 피드 수집 중...")
        rss_videos = parse_rss_feed(channel_id, max_videos=15)
        print(f"  ✓ RSS에서 {len(rss_videos)}개 영상 수집")

        # Shorts 채널 체크
        if not rss_videos or len(rss_videos) < 5:
            shorts_videos = get_shorts_channel_data(channel_id, row_number)
            if shorts_videos:
                rss_videos.extend(shorts_videos)

        if not rss_videos:
            print(f"  ⚠️  수집된 영상이 없습니다")
            return result

        # 중복 제거 및 정렬
        unique_videos = {}
        for video in rss_videos:
            if video['video_id']:
                unique_videos[video['video_id']] = video
        
        rss_videos = sorted(
            unique_videos.values(),
            key=lambda v: v['published_at'] or datetime.now(timezone.utc),
            reverse=True
        )[:30]

        # 영상 정보 처리
        dates = []
        video_infos = []

        for video in rss_videos:
            if video['published_at']:
                dates.append(video['published_at'])
            
            video_infos.append({
                'id': video['video_id'],
                'title': video['title'],
                'thumbnail_url': video.get('thumbnail_url', '')
            })

        # 썸네일 URL 추출
        result['video_links'] = get_thumbnail_urls(video_infos, max_count=5)
        print(f"  ✅ 썸네일 URL 수집 완료: {len([u for u in result['video_links'] if u])}개")

        # 최초/최근 업로드 계산 (⭐⭐⭐ 수정: K열 - L열 기준)
        if dates:
            latest_date = max(dates)      # L열: 최근 업로드
            first_date = min(dates)       # K열: 최초 업로드
            
            result['latest_upload'] = latest_date.strftime('%Y-%m-%d')
            result['first_upload'] = first_date.strftime('%Y-%m-%d')
            
            # ⭐⭐⭐ 핵심 수정: 운영기간 = 최근 업로드 - 최초 업로드
            result['operation_days'] = max(0, (latest_date - first_date).days)
            
            print(f"  ✅ 최초업로드 (K열): {result['first_upload']}")
            print(f"  ✅ 최근업로드 (L열): {result['latest_upload']}")
            print(f"  🔍 운영기간: {result['operation_days']}일")

        # 조회수 계산 (RSS는 조회수 미제공)
        now = datetime.now(timezone.utc)
        
        views_5d_list = []
        views_10d_list = []
        views_15d_list = []

        print(f"  📅 날짜 기준 영상 수 계산 (기준: {now.strftime('%Y-%m-%d %H:%M UTC')})")

        for video in rss_videos:
            if not video['published_at']:
                continue

            try:
                days_ago = (now - video['published_at']).days

                if days_ago <= 5:
                    views_5d_list.append(video)
                if days_ago <= 10:
                    views_10d_list.append(video)
                if days_ago <= 15:
                    views_15d_list.append(video)
            except Exception as e:
                print(f"  ⚠️  계산 실패: {e}")
                continue

        result['count_5d'] = len(views_5d_list)
        result['count_10d'] = len(views_10d_list)
        result['views_5d'] = 0  # RSS는 조회수 미제공
        result['views_10d'] = 0
        result['views_15d'] = 0

        print(f"  ✅ 5일: {result['count_5d']}개 영상")
        print(f"  ✅ 10일: {result['count_10d']}개 영상")

        # 영상 수
        result['video_count'] = len(rss_videos)

        return result

    except Exception as e:
        print(f"  ❌ 데이터 수집 실패: {e}")
        traceback.print_exc()
        return None

# ========================================
# 10. 수동 입력 컬럼 보존 (배치 읽기 방식)
# ========================================

def preserve_manual_columns_batch(all_sheet_data, row_num):
    """배치 읽기된 데이터에서 수동 컬럼 값 추출"""
    try:
        row_idx = row_num - 1
        if row_idx >= len(all_sheet_data):
            return {col: '' for col in MANUAL_INPUT_COLUMNS}
        
        row_data = all_sheet_data[row_idx]
        manual_values = {}
        
        for col in MANUAL_INPUT_COLUMNS:
            cell_value = row_data[col - 1] if len(row_data) >= col else ''
            manual_values[col] = cell_value if cell_value else ''
        
        return manual_values
    except Exception as e:
        print(f"⚠️ 수동 컬럼 추출 실패: {e}")
        return {col: '' for col in MANUAL_INPUT_COLUMNS}

# ========================================
# 11. 배치 업데이트 (20행씩)
# ========================================
def build_cell_list(row_num, data_dict, manual_values, row_data):
    """행 데이터를 셀 리스트로 변환"""
    cell_list = []
    
    try:
        # 기존 데이터 읽기
        existing_url = row_data[COL_URL - 1] if len(row_data) >= COL_URL else ''
        existing_video_count = str(row_data[COL_VIDEO_COUNT - 1]).strip() if len(row_data) >= COL_VIDEO_COUNT else ''
        existing_total_views = str(row_data[COL_TOTAL_VIEWS - 1]).strip() if len(row_data) >= COL_TOTAL_VIEWS else ''
        
        # ✅ [수정] 국가(D열) 처리 로직
        existing_country = str(row_data[COL_COUNTRY - 1]).strip() if len(row_data) >= COL_COUNTRY else ''
        api_country = data_dict.get('country', '')
        
        final_country = api_country  # 기본값: API에서 가져온 값 (빈 셀일 경우)

        if existing_country:
            # 기존 값이 있는 경우
            if existing_country.upper() in COUNTRY_MAP:
                # 영어 코드(예: US, KR)인 경우 → 한글로 변환하여 업데이트
                final_country = COUNTRY_MAP[existing_country.upper()]
            else:
                # 이미 한글이거나 그 외의 값인 경우 → 기존 값 유지 (업데이트 X)
                final_country = existing_country

        columns_data = [
            (COL_CHANNEL_NAME, data_dict.get('channel_name', '')),
            (COL_URL, existing_url),
            (COL_HANDLE, data_dict.get('handle', '')),
            (COL_COUNTRY, final_country),  # ✅ 수정된 국가 값 적용
            (COL_SUBSCRIBERS, data_dict.get('subscribers', 0)),
            (COL_VIDEO_COUNT, data_dict.get('video_count', 0) if not existing_video_count else existing_video_count),
            (COL_TOTAL_VIEWS, data_dict.get('total_views', 0) if not existing_total_views else existing_total_views),
            (COL_FIRST_UPLOAD, data_dict.get('first_upload', '')),
            (COL_LATEST_UPLOAD, data_dict.get('latest_upload', '')),
            (COL_COLLECT_DATE, data_dict.get('collect_date', '')),
            (COL_VIEWS_5_TOTAL, data_dict.get('views_5', 0)),
            (COL_VIEWS_10_TOTAL, data_dict.get('views_10', 0)),
            (COL_VIEWS_20_TOTAL, data_dict.get('views_20', 0)),
            (COL_VIEWS_30_TOTAL, data_dict.get('views_30', 0)),
            (COL_OPERATION_DAYS, data_dict.get('operation_days', 0)),
            (COL_COUNT_5D, data_dict.get('count_5d', 0)),
            (COL_COUNT_10D, data_dict.get('count_10d', 0)),
            (COL_CHANNEL_ID, data_dict.get('channel_id', '')),
            (COL_VIEWS_5D, data_dict.get('views_5d', 0)),
            (COL_VIEWS_10D, data_dict.get('views_10d', 0)),
            (COL_VIEWS_15D, data_dict.get('views_15d', 0)),
            (COL_YT_CATEGORY, data_dict.get('yt_category', '미분류')),
            (COL_CHANNEL_THUMBNAIL, data_dict.get('channel_thumbnail', '')),  # ✅ 추가
        ]
        
        for col_idx, value in columns_data:
            # 0인 경우도 기록해야 하므로 value가 None이 아니면 추가
            if value or value == 0:
                cell_list.append(gspread.Cell(row_num, col_idx, value))
        
        video_links = data_dict.get('video_links', [''] * 5)
        for i, col_idx in enumerate(COL_VIDEO_LINKS):
            if video_links[i]:
                cell_list.append(gspread.Cell(row_num, col_idx, video_links[i]))
        
        for col, value in manual_values.items():
            if value:
                cell_list.append(gspread.Cell(row_num, col, value))
        
        return cell_list
    
    except Exception as e:
        print(f"❌ 셀 리스트 생성 실패 (Row {row_num}): {e}")
        traceback.print_exc()
        return []

# ========================================
# 12. 메인 실행
# ========================================
def main():
    """메인 실행 함수"""
    print("=" * 80)
    print("📂 YouTube 채널 분석기 v2 - GitHub Actions 버전 (API 제거)")
    print("🔥 RSS + yt-dlp 하이브리드 방식 (API 호출 0회, 무제한 실행 가능)")
    print("=" * 80)

    try:
        print("\n📊 시트 연결 중...")
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            SERVICE_ACCOUNT_FILE, scope)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(DATA_TAB_NAME)

        print("✅ 시트 연결 완료\n")

        print("=" * 80)
        range_input = os.environ.get('RANGE', '').strip()

        if range_input:
            if '-' in range_input:
                start_row, end_row = map(int, range_input.split('-'))
            else:
                start_row = end_row = int(range_input)
            print(f"✅ 환경변수에서 범위 읽기: {start_row}행 ~ {end_row}행")
        else:
            all_data = worksheet.get_all_values()
            start_row = 2
            end_row = len(all_data)
            print(f"✅ 전체 처리: {start_row}행 ~ {end_row}행")

        print(f"📌 총 {end_row - start_row + 1}개 행 처리 예정")
        print(f"📦 배치 크기: {BATCH_SIZE}행씩 처리\n")

        print("📥 시트 데이터 일괄 로드 중...")
        all_sheet_data = worksheet.get_all_values()
        print(f"✅ {len(all_sheet_data)}행 데이터 로드 완료 (읽기 요청: 1회)\n")

        print("=" * 80)
        print("🚀 채널 분석 시작")
        print("=" * 80)

        success_count = 0
        fail_count = 0
        start_time = time.time()
        
        batch_cells = []
        batch_rows_count = 0

        for row_num in range(start_row, end_row + 1):
            print(f"\n{'='*80}")
            print(f"🔍 [{row_num - start_row + 1}/{end_row - start_row + 1}] 처리 중...")
            print(f"{'='*80}")

            try:
                row_idx = row_num - 1
                if row_idx >= len(all_sheet_data):
                    print(f"⏭️  Row {row_num}: 데이터 없음")
                    continue
                
                row_data = all_sheet_data[row_idx]
                
                if len(row_data) < 3:
                    print(f"⏭️  Row {row_num}: 데이터 부족")
                    continue

                url = row_data[COL_URL - 1] if len(row_data) >= COL_URL else ''
                handle = row_data[COL_HANDLE - 1] if len(row_data) >= COL_HANDLE else ''

                if not url and not handle:
                    print(f"⏭️  Row {row_num}: URL/핸들 없음")
                    continue

                print(f"📌 URL: {url}")
                print(f"📌 핸들: {handle}")

                manual_values = preserve_manual_columns_batch(all_sheet_data, row_num)

                data = get_channel_data_hybrid(url, row_num, row_data, worksheet)

                if not data:
                    print(f"❌ Row {row_num}: 데이터 수집 실패")
                    fail_count += 1
                    continue

                cells = build_cell_list(row_num, data, manual_values, row_data)
                batch_cells.extend(cells)
                batch_rows_count += 1
                success_count += 1
                
                print(f"✅ Row {row_num} 준비 완료 ({len(cells)}개 셀)")

                if batch_rows_count >= BATCH_SIZE or row_num == end_row:
                    if batch_cells:
                        print(f"\n📤 배치 업데이트 실행: {batch_rows_count}행, {len(batch_cells)}개 셀")
                        worksheet.update_cells(batch_cells)
                        print(f"✅ 배치 업데이트 완료!")
                        batch_cells = []
                        batch_rows_count = 0
                        
                        print(f"💤 2초 대기...")
                        time.sleep(2)
                
                time.sleep(3)

            except Exception as e:
                print(f"❌ Row {row_num} 처리 중 오류: {e}")
                traceback.print_exc()
                fail_count += 1
                time.sleep(5)
                continue

        if batch_cells:
            print(f"\n📤 최종 배치 업데이트: {batch_rows_count}행, {len(batch_cells)}개 셀")
            worksheet.update_cells(batch_cells)
            print(f"✅ 최종 배치 업데이트 완료!")

        elapsed_time = time.time() - start_time
        print("\n" + "=" * 80)
        print("📊 최종 결과")
        print("=" * 80)
        print(f"✅ 성공: {success_count}개")
        print(f"❌ 실패: {fail_count}개")
        print(f"⏱️  소요 시간: {elapsed_time / 60:.1f}분")
        if (success_count + fail_count) > 0:
            print(f"⚡ 평균 속도: {elapsed_time / (success_count + fail_count):.1f}초/채널")
        print(f"🎉 API 호출: 0회 (완전 제거) - 무제한 실행 가능!")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        traceback.print_exc()

# ========================================
# 13. 실행
# ========================================
if __name__ == '__main__':
    main()
