# ========================================
# YouTube 채널 분석기 v2 - GitHub Actions 버전 (고정)
# RSS + yt-dlp 하이브리드 방식 (API 호출 제거)
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
import sys

# ========================================
# 설정 변수
# ========================================

SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT')
if not SERVICE_ACCOUNT_JSON:
    raise Exception("❌ GOOGLE_SERVICE_ACCOUNT 환경변수가 설정되지 않았습니다")

# JSON을 임시 파일로 저장
with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
    f.write(SERVICE_ACCOUNT_JSON)
    SERVICE_ACCOUNT_FILE = f.name

def cleanup_temp_file():
    try:
        if SERVICE_ACCOUNT_FILE and os.path.exists(SERVICE_ACCOUNT_FILE):
            os.remove(SERVICE_ACCOUNT_FILE)
            print("✅ 임시 파일 정리 완료")
    except Exception as e:
        print(f"⚠️ 임시 파일 정리 실패: {e}")

atexit.register(cleanup_temp_file)

SHEET_NAME = os.environ.get('SHEET_NAME', '유튜브보물창고_테스트')
DATA_TAB_NAME = os.environ.get('DATA_TAB_NAME', '데이터')
BATCH_SIZE = 20

# 컬럼 매핑
COL_CHANNEL_NAME = 1
COL_URL = 2
COL_HANDLE = 3
COL_COUNTRY = 4
COL_CATEGORY_1 = 5
COL_CATEGORY_2 = 6
COL_MEMO = 7
COL_SUBSCRIBERS = 8
COL_VIDEO_COUNT = 9
COL_TOTAL_VIEWS = 10
COL_FIRST_UPLOAD = 11
COL_LATEST_UPLOAD = 12
COL_COLLECT_DATE = 13
COL_VIEWS_5_TOTAL = 14
COL_VIEWS_10_TOTAL = 15
COL_VIEWS_20_TOTAL = 16
COL_VIEWS_30_TOTAL = 17
COL_KEYWORD = 18
COL_NOTE = 19
COL_OPERATION_DAYS = 20
COL_TEMPLATE = 21
COL_COUNT_5D = 22
COL_COUNT_10D = 23
COL_CHANNEL_ID = 24
COL_VIEWS_5D = 25
COL_VIEWS_10D = 26
COL_VIEWS_15D = 27
COL_YT_CATEGORY = 28
COL_VIDEO_LINKS = [29, 30, 31, 32, 33]
COL_CHANNEL_THUMBNAIL = 34

MANUAL_INPUT_COLUMNS = [COL_CATEGORY_1, COL_CATEGORY_2, COL_MEMO, 
                        COL_KEYWORD, COL_NOTE, COL_TEMPLATE]

COUNTRY_MAP = {
    'KR': '한국', 'US': '미국', 'JP': '일본', 'GB': '영국', 
    'DE': '독일', 'FR': '프랑스', 'CA': '캐나다', 'AU': '호주',
    'VN': '베트남', 'TH': '태국', 'ID': '인도네시아', 'IN': '인도',
    'BR': '브라질', 'MX': '멕시코', 'RU': '러시아', 'TR': '터키',
    'ES': '스페인', 'IT': '이탈리아', 'TW': '대만', 'HK': '홍콩',
    'PH': '필리핀', 'CN': '중국', 'SG': '싱가포르', 'MY': '말레이시아'
}

CATEGORY_MAP = {
    '1': '영화/애니메이션', '2': '자동차/차량', '10': '음악',
    '15': '반려동물/동물', '17': '스포츠', '18': '단편 동영상',
    '19': '여행/이벤트', '20': '게임', '21': '브이로그',
    '22': '인물/블로그', '23': '코미디', '24': '엔터테인먼트',
    '25': '뉴스/정치', '26': '노하우/스타일', '27': '교육',
    '28': '과학기술', '29': '비영리/사회운동'
}

# ========================================
# 헬퍼 함수
# ========================================

def get_country_name(country_code):
    if not country_code or country_code.strip() == '':
        return '한국'
    return COUNTRY_MAP.get(country_code.upper(), country_code)

def get_category_name(category_id):
    if not category_id:
        return '미분류'
    return CATEGORY_MAP.get(str(category_id), '미분류')

def get_thumbnail_urls(video_infos, max_count=5):
    urls = []
    for video_info in video_infos[:max_count]:
        try:
            if video_info.get('thumbnail_url'):
                urls.append(video_info['thumbnail_url'])
            else:
                urls.append('')
        except:
            urls.append('')
    
    while len(urls) < max_count:
        urls.append('')
    
    return urls

def parse_published_date(date_str):
    if not date_str:
        return None
    try:
        dt = dateutil_parser.parse(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except:
        return None

MAX_RETRIES = 3
RETRY_DELAY = 2

def retry_with_backoff(func):
    def wrapper(*args, **kwargs):
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (2 ** attempt)
                    print(f"  ⚠️  재시도 ({attempt + 1}/{MAX_RETRIES})... {wait_time}초 대기")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
        
        raise Exception(f"❌ {MAX_RETRIES}회 재시도 후 실패")
    
    return wrapper

# ========================================
# RSS 파서
# ========================================

@retry_with_backoff
def parse_rss_feed(channel_id, max_videos=15):
    """YouTube RSS 피드에서 최근 영상 정보 추출"""
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"

    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    })
    
    retry = Retry(total=2, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    try:
        print(f"  📡 RSS 요청 중... (타임아웃: 10초)")
        sys.stdout.flush()
        
        response = session.get(rss_url, timeout=10)
        response.raise_for_status()
        
        print(f"  📄 RSS 파싱 중...")
        sys.stdout.flush()
        
        feed = feedparser.parse(response.content)

        if not feed.entries:
            print(f"  ⚠️  RSS 피드 비어있음")
            return []

        videos = []
        for entry in feed.entries[:max_videos]:
            try:
                video_id = entry.yt_videoid if hasattr(entry, 'yt_videoid') else None
                if not video_id and 'id' in entry:
                    video_id = entry.id.split(':')[-1]

                published_str = entry.published if hasattr(entry, 'published') else None
                published_at = parse_published_date(published_str)

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
            except:
                continue

        print(f"  ✅ RSS 완료: {len(videos)}개 영상")
        sys.stdout.flush()
        return videos

    except requests.exceptions.Timeout:
        print(f"  ❌ RSS 타임아웃")
        return []
    except Exception as e:
        print(f"  ❌ RSS 오류: {str(e)[:100]}")
        return []

# ========================================
# yt-dlp 함수
# ========================================

def get_channel_info_ytdlp(channel_url):
    """yt-dlp로 채널 정보 추출"""
    try:
        if '@' in channel_url:
            handle = channel_url.split('@')[-1].split('/')[0]
            channel_url = f"https://www.youtube.com/@{handle}"
        elif '/channel/' not in channel_url:
            channel_url = channel_url if channel_url.startswith('http') else f"https://www.youtube.com{channel_url}"
        
        print(f"  🎬 yt-dlp 실행 중...")
        sys.stdout.flush()
        
        result = subprocess.run(
            ['yt-dlp', '--dump-json', '--no-warnings', '-e', 'generic', channel_url],
            capture_output=True,
            text=True,
            timeout=15
        )
        
        if result.returncode != 0:
            print(f"  ⚠️  yt-dlp 실패 (코드: {result.returncode})")
            return None
        
        if not result.stdout.strip():
            print(f"  ⚠️  yt-dlp 출력 없음")
            return None
        
        data = json.loads(result.stdout)
        
        channel_info = {
            'channel_id': data.get('channel_id', ''),
            'channel_name': data.get('uploader', '') or data.get('channel', ''),
            'handle': data.get('channel_url', '').split('@')[-1] if data.get('channel_url') else '',
            'thumbnail_url': data.get('thumbnail', ''),
        }
        
        print(f"  ✅ yt-dlp 완료")
        sys.stdout.flush()
        return channel_info
    
    except subprocess.TimeoutExpired:
        print(f"  ❌ yt-dlp 타임아웃 (15초)")
        return None
    except FileNotFoundError:
        print(f"  ❌ yt-dlp 설치 안 됨")
        return None
    except Exception as e:
        print(f"  ❌ yt-dlp 오류: {str(e)[:100]}")
        return None

# ========================================
# 채널 ID 추출
# ========================================

def extract_channel_id_from_url(channel_url, row_number, row_data=None):
    """채널 URL에서 channel_id 추출"""
    if '/channel/' in channel_url:
        return channel_url.split('/channel/')[-1].split('/')[0].split('?')[0]

    if row_data and len(row_data) >= COL_HANDLE:
        handle_from_sheet = str(row_data[COL_HANDLE - 1]).strip()
        if handle_from_sheet:
            if handle_from_sheet.startswith('@'):
                handle_from_sheet = handle_from_sheet[1:]
            
            try:
                channel_url_for_handle = f"https://www.youtube.com/@{handle_from_sheet}"
                channel_info = get_channel_info_ytdlp(channel_url_for_handle)
                if channel_info and channel_info.get('channel_id'):
                    return channel_info['channel_id']
            except:
                pass

    decoded_url = urllib.parse.unquote(channel_url)
    handle_match = re.search(r'@([^/\s?]+)', decoded_url)
    
    if not handle_match:
        return None

    handle = handle_match.group(1)

    try:
        channel_url_for_handle = f"https://www.youtube.com/@{handle}"
        channel_info = get_channel_info_ytdlp(channel_url_for_handle)
        if channel_info and channel_info.get('channel_id'):
            return channel_info['channel_id']
    except:
        pass

    return None

# ========================================
# Shorts 채널 데이터
# ========================================

def get_shorts_channel_data(channel_id, row_number):
    """Shorts 전용 채널에서 영상 데이터 수집"""
    try:
        shorts_url = f"https://www.youtube.com/channel/{channel_id}/shorts"
        
        result = subprocess.run(
            ['yt-dlp', '--dump-json', '--no-warnings', '-e', 'generic',
             '--playlist-items', '1:30', shorts_url],
            capture_output=True,
            text=True,
            timeout=15
        )
        
        if result.returncode == 0:
            shorts_videos = []
            for line in result.stdout.strip().split('\n'):
                try:
                    data = json.loads(line)
                    published_str = data.get('upload_date', '')
                    if published_str and len(published_str) == 8:
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
                print(f"  ✓ Shorts: {len(shorts_videos)}개")
            
            return shorts_videos
    except:
        pass
    
    return []

# ========================================
# 메인 데이터 수집
# ========================================

def get_channel_data_hybrid(channel_url, row_number, row_data, worksheet):
    """RSS + yt-dlp 하이브리드 방식으로 채널 데이터 수집"""
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
        'channel_thumbnail': '',
        'video_links': ['', '', '', '', '']
    }

    try:
        existing_channel_id = ''
        if len(row_data) >= COL_CHANNEL_ID:
            existing_channel_id = str(row_data[COL_CHANNEL_ID - 1]).strip()

        channel_id = existing_channel_id

        if not channel_id:
            print(f"  📍 channel_id 검색 중...")
            sys.stdout.flush()
            
            channel_id = extract_channel_id_from_url(channel_url, row_number, row_data)

            if not channel_id:
                print(f"  ❌ channel_id 추출 실패")
                return None

            try:
                cell_list = [gspread.Cell(row_number, COL_CHANNEL_ID, channel_id)]
                worksheet.update_cells(cell_list)
                print(f"  ✅ channel_id 저장: {channel_id}")
                sys.stdout.flush()
                time.sleep(1)
            except Exception as e:
                print(f"  ⚠️  저장 실패: {e}")
        else:
            print(f"  ✓ channel_id 사용: {channel_id}")
            sys.stdout.flush()

        result['channel_id'] = channel_id

        print(f"  🎬 채널 정보 추출 중...")
        sys.stdout.flush()
        
        channel_info = get_channel_info_ytdlp(channel_url)
        
        if channel_info:
            result['channel_name'] = channel_info.get('channel_name', '')
            result['handle'] = channel_info.get('handle', '')
            result['channel_thumbnail'] = channel_info.get('thumbnail_url', '')

        print(f"  📡 RSS 수집 중...")
        sys.stdout.flush()
        
        rss_videos = parse_rss_feed(channel_id, max_videos=15)

        if not rss_videos or len(rss_videos) < 5:
            print(f"  🎬 Shorts 확인 중...")
            sys.stdout.flush()
            
            shorts_videos = get_shorts_channel_data(channel_id, row_number)
            if shorts_videos:
                rss_videos.extend(shorts_videos)

        if not rss_videos:
            print(f"  ⚠️  영상 없음")
            return result

        # 중복 제거
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

        # 썸네일
        result['video_links'] = get_thumbnail_urls(video_infos, max_count=5)

        # 최초/최근 업로드 계산
        if dates:
            latest_date = max(dates)
            first_date = min(dates)
            
            result['latest_upload'] = latest_date.strftime('%Y-%m-%d')
            result['first_upload'] = first_date.strftime('%Y-%m-%d')
            result['operation_days'] = max(0, (latest_date - first_date).days)

        # 날짜별 계산
        now = datetime.now(timezone.utc)
        
        count_5d = 0
        count_10d = 0

        for video in rss_videos:
            if not video['published_at']:
                continue

            days_ago = (now - video['published_at']).days

            if days_ago <= 5:
                count_5d += 1
            if days_ago <= 10:
                count_10d += 1

        result['count_5d'] = count_5d
        result['count_10d'] = count_10d
        result['video_count'] = len(rss_videos)

        print(f"  ✅ 수집 완료: {result['channel_name']}")
        sys.stdout.flush()
        
        return result

    except Exception as e:
        print(f"  ❌ 오류: {e}")
        traceback.print_exc()
        return None

# ========================================
# 수동 컬럼 보존
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
    except:
        return {col: '' for col in MANUAL_INPUT_COLUMNS}

# ========================================
# 셀 리스트 생성
# ========================================

def build_cell_list(row_num, data_dict, manual_values, row_data):
    """행 데이터를 셀 리스트로 변환"""
    cell_list = []
    
    try:
        existing_url = row_data[COL_URL - 1] if len(row_data) >= COL_URL else ''
        existing_video_count = str(row_data[COL_VIDEO_COUNT - 1]).strip() if len(row_data) >= COL_VIDEO_COUNT else ''
        existing_total_views = str(row_data[COL_TOTAL_VIEWS - 1]).strip() if len(row_data) >= COL_TOTAL_VIEWS else ''
        existing_country = str(row_data[COL_COUNTRY - 1]).strip() if len(row_data) >= COL_COUNTRY else ''
        
        if existing_country:
            if existing_country.upper() in COUNTRY_MAP:
                final_country = COUNTRY_MAP[existing_country.upper()]
            else:
                final_country = existing_country
        else:
            final_country = data_dict.get('country', '')

        columns_data = [
            (COL_CHANNEL_NAME, data_dict.get('channel_name', '')),
            (COL_URL, existing_url),
            (COL_HANDLE, data_dict.get('handle', '')),
            (COL_COUNTRY, final_country),
            (COL_SUBSCRIBERS, 0),
            (COL_VIDEO_COUNT, data_dict.get('video_count', 0) or existing_video_count),
            (COL_TOTAL_VIEWS, existing_total_views if existing_total_views else 0),
            (COL_FIRST_UPLOAD, data_dict.get('first_upload', '')),
            (COL_LATEST_UPLOAD, data_dict.get('latest_upload', '')),
            (COL_COLLECT_DATE, data_dict.get('collect_date', '')),
            (COL_VIEWS_5_TOTAL, 0),
            (COL_VIEWS_10_TOTAL, 0),
            (COL_VIEWS_20_TOTAL, 0),
            (COL_VIEWS_30_TOTAL, 0),
            (COL_OPERATION_DAYS, data_dict.get('operation_days', 0)),
            (COL_COUNT_5D, data_dict.get('count_5d', 0)),
            (COL_COUNT_10D, data_dict.get('count_10d', 0)),
            (COL_CHANNEL_ID, data_dict.get('channel_id', '')),
            (COL_VIEWS_5D, 0),
            (COL_VIEWS_10D, 0),
            (COL_VIEWS_15D, 0),
            (COL_YT_CATEGORY, '미분류'),
            (COL_CHANNEL_THUMBNAIL, data_dict.get('channel_thumbnail', '')),
        ]
        
        for col_idx, value in columns_data:
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
        print(f"❌ 셀 생성 실패: {e}")
        return []

# ========================================
# 메인 함수
# ========================================

def main():
    """메인 실행 함수"""
    print("=" * 80)
    print("📂 YouTube 채널 분석기 v2")
    print("=" * 80)
    print(f"⏰ 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    sys.stdout.flush()

    try:
        print("📊 Google Sheets 연결 중...")
        sys.stdout.flush()
        
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            SERVICE_ACCOUNT_FILE, scope)
        gc = gspread.authorize(creds)
        
        print("✅ 인증 완료")
        print(f"📊 시트 '{SHEET_NAME}' 열기 중...")
        sys.stdout.flush()
        
        spreadsheet = gc.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(DATA_TAB_NAME)

        print("✅ 시트 연결 완료\n")
        sys.stdout.flush()

        range_input = os.environ.get('RANGE', '').strip()

        if range_input:
            if '-' in range_input:
                start_row, end_row = map(int, range_input.split('-'))
            else:
                start_row = end_row = int(range_input)
            print(f"✅ 범위: {start_row}~{end_row}")
        else:
            all_data = worksheet.get_all_values()
            start_row = 2
            end_row = len(all_data)
            print(f"✅ 전체: {start_row}~{end_row}")

        print(f"📥 시트 데이터 로드 중...")
        sys.stdout.flush()
        
        all_sheet_data = worksheet.get_all_values()
        print(f"✅ {len(all_sheet_data)}행 로드\n")
        sys.stdout.flush()

        print("=" * 80)
        print("🚀 채널 분석 시작")
        print("=" * 80 + "\n")
        sys.stdout.flush()

        success_count = 0
        fail_count = 0
        start_time = time.time()
        
        batch_cells = []
        batch_rows_count = 0

        for row_num in range(start_row, end_row + 1):
            try:
                row_idx = row_num - 1
                if row_idx >= len(all_sheet_data):
                    continue
                
                row_data = all_sheet_data[row_idx]
                
                if len(row_data) < 3:
                    continue

                url = row_data[COL_URL - 1] if len(row_data) >= COL_URL else ''
                handle = row_data[COL_HANDLE - 1] if len(row_data) >= COL_HANDLE else ''

                if not url and not handle:
                    continue

                print(f"\n[{row_num - start_row + 1}/{end_row - start_row + 1}] Row {row_num}")
                print(f"  URL: {url[:50]}...")
                sys.stdout.flush()

                manual_values = preserve_manual_columns_batch(all_sheet_data, row_num)

                data = get_channel_data_hybrid(url, row_num, row_data, worksheet)

                if not data:
                    fail_count += 1
                    print(f"  ❌ 실패")
                    sys.stdout.flush()
                    continue

                cells = build_cell_list(row_num, data, manual_values, row_data)
                batch_cells.extend(cells)
                batch_rows_count += 1
                success_count += 1

                if batch_rows_count >= BATCH_SIZE or row_num == end_row:
                    if batch_cells:
                        print(f"\n📤 배치 업데이트: {batch_rows_count}행")
                        sys.stdout.flush()
                        
                        worksheet.update_cells(batch_cells)
                        print(f"✅ 완료")
                        sys.stdout.flush()
                        
                        batch_cells = []
                        batch_rows_count = 0
                        time.sleep(2)
                
                time.sleep(2)

            except Exception as e:
                print(f"  ❌ 오류: {str(e)[:100]}")
                sys.stdout.flush()
                fail_count += 1
                time.sleep(3)
                continue

        if batch_cells:
            print(f"\n📤 최종 배치 업데이트")
            sys.stdout.flush()
            
            worksheet.update_cells(batch_cells)
            print(f"✅ 완료")
            sys.stdout.flush()

        elapsed_time = time.time() - start_time
        
        print("\n" + "=" * 80)
        print("📊 최종 결과")
        print("=" * 80)
        print(f"✅ 성공: {success_count}")
        print(f"❌ 실패: {fail_count}")
        print(f"⏱️  시간: {elapsed_time / 60:.1f}분")
        print(f"🎉 완료!")
        print("=" * 80)
        sys.stdout.flush()

    except Exception as e:
        print(f"\n❌ 오류: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
