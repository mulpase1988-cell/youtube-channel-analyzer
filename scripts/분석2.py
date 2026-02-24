# ========================================
# YouTube 채널 분석기 v2 - GitHub Actions 버전
# ✅ RSS 전용 모드 (API 호출 완전 제거)
# ✅ 강화된 RSS 파싱 (User-Agent + 재시도)
# ✅ yt-dlp 백업 방식
# ========================================

# ========================================
# 1. 라이브러리 임포트
# ========================================
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta
from dateutil import parser as dateutil_parser
import feedparser
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import subprocess
import json
import time
import re
import urllib.parse
import traceback
import os
import tempfile
import random
import atexit

# ========================================
# 2. 설정 변수
# ========================================

# 🔥 환경변수에서 인증 정보 로드
SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT')
if not SERVICE_ACCOUNT_JSON:
    raise Exception("❌ GOOGLE_SERVICE_ACCOUNT 환경변수가 설정되지 않았습니다")

# JSON을 임시 파일로 저장
SERVICE_ACCOUNT_FILE = None

def cleanup_temp_file():
    """임시 파일 정리"""
    global SERVICE_ACCOUNT_FILE
    if SERVICE_ACCOUNT_FILE and os.path.exists(SERVICE_ACCOUNT_FILE):
        try:
            os.remove(SERVICE_ACCOUNT_FILE)
        except:
            pass

atexit.register(cleanup_temp_file)

with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
    f.write(SERVICE_ACCOUNT_JSON)
    SERVICE_ACCOUNT_FILE = f.name

# Google Sheets 설정 (환경변수 우선, 없으면 기본값)
SHEET_NAME = os.environ.get('SHEET_NAME', '유튜브보물창고_테스트')
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
COL_CHANNEL_THUMBNAIL = 34  # AH: 채널 썸네일

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

# ========================================
# 3. 헬퍼 함수들
# ========================================
def get_country_name(country_code):
    """국가 코드를 한글명으로 변환 (빈 값이면 '한국' 기본값)"""
    if not country_code or country_code.strip() == '':
        return '한국'
    return COUNTRY_MAP.get(country_code.upper(), country_code)

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
# 4. 강화된 RSS 파싱 (User-Agent + 재시도)
# ========================================
def parse_rss_feed_robust(channel_id, max_videos=30):
    """
    강화된 RSS 피드 파싱
    ✅ User-Agent 포함
    ✅ 재시도 로직
    ✅ Timeout 설정
    """
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    
    print(f"  📡 RSS 요청 중...")
    
    # ✅ Session 생성 (재사용 가능)
    session = requests.Session()
    
    # ✅ User-Agent 설정 (봇 차단 우회)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    # ✅ 재시도 로직 설정
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    try:
        # ✅ 요청 실행 (User-Agent + 타임아웃)
        response = session.get(
            rss_url,
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            print(f"  ✓ RSS 요청 성공 (상태: {response.status_code})")
        else:
            print(f"  ⚠️  RSS 요청 실패: {response.status_code}")
            return []
        
        # ✅ feedparser로 파싱
        feed = feedparser.parse(response.content)
        
        if not feed.entries:
            print(f"  ⚠️  RSS 피드가 비어있음")
            return []
        
        videos = []
        for entry in feed.entries[:max_videos]:
            try:
                video_id = entry.yt_videoid if hasattr(entry, 'yt_videoid') else None
                if not video_id and 'id' in entry:
                    video_id = entry.id.split(':')[-1]
                
                published_str = entry.published if hasattr(entry, 'published') else None
                published_at = None
                
                if published_str:
                    try:
                        published_at = datetime.fromisoformat(
                            published_str.replace('Z', '+00:00')
                        )
                    except:
                        pass
                
                videos.append({
                    'video_id': video_id,
                    'title': entry.title if hasattr(entry, 'title') else '',
                    'published_at': published_at
                })
            except Exception as e:
                print(f"  ⚠️  RSS 항목 파싱 실패: {e}")
                continue
        
        print(f"  ✓ {len(videos)}개 영상 추출")
        return videos
    
    except requests.exceptions.Timeout:
        print(f"  ❌ RSS 요청 Timeout (10초)")
        return []
    except requests.exceptions.ConnectionError as e:
        print(f"  ❌ RSS 연결 실패: {e}")
        return []
    except Exception as e:
        print(f"  ❌ RSS 파싱 실패: {e}")
        traceback.print_exc()
        return []
    finally:
        session.close()

# ========================================
# 5. 채널 ID 추출 (yt-dlp 방식)
# ========================================
def extract_channel_id_from_url(channel_url, row_data=None):
    """채널 URL에서 channel_id 추출 (URL 분석 + yt-dlp)"""
    
    # ✅ Step 1: URL에서 직접 channel_id 추출
    if '/channel/' in channel_url:
        channel_id = channel_url.split('/channel/')[-1].split('/')[0].split('?')[0]
        if channel_id:
            print(f"  ✓ URL에서 channel_id 추출: {channel_id}")
            return channel_id
    
    # ✅ Step 2: 핸들에서 channel_id 추출 시도 (yt-dlp)
    decoded_url = urllib.parse.unquote(channel_url)
    handle_match = re.search(r'@([^/\s?]+)', decoded_url)
    
    if handle_match:
        handle = handle_match.group(1)
        print(f"  📍 핸들 추출: @{handle}")
        
        try:
            result = subprocess.run(
                ['yt-dlp', '--dump-json', '--no-warnings', 
                 f'https://www.youtube.com/@{handle}', '--playlist-items', '1'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                data = json.loads(result.stdout)
                channel_id = data.get('channel_id')
                if channel_id:
                    print(f"  ✓ yt-dlp에서 channel_id 추출: {channel_id}")
                    return channel_id
        except Exception as e:
            print(f"  ⚠️  yt-dlp 추출 실패: {e}")
    
    # ✅ Step 3: C열 핸들 사용
    if row_data and len(row_data) >= COL_HANDLE:
        handle_from_sheet = str(row_data[COL_HANDLE - 1]).strip()
        if handle_from_sheet and handle_from_sheet.startswith('@'):
            handle_from_sheet = handle_from_sheet[1:]
        
        if handle_from_sheet:
            print(f"  📋 C열에서 핸들 사용: @{handle_from_sheet}")
            try:
                result = subprocess.run(
                    ['yt-dlp', '--dump-json', '--no-warnings',
                     f'https://www.youtube.com/@{handle_from_sheet}', '--playlist-items', '1'],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    data = json.loads(result.stdout)
                    channel_id = data.get('channel_id')
                    if channel_id:
                        print(f"  ✓ C열 핸들로 channel_id 추출: {channel_id}")
                        return channel_id
            except Exception as e:
                print(f"  ⚠️  C열 핸들 yt-dlp 실패: {e}")
    
    print(f"  ❌ channel_id를 추출할 수 없습니다")
    return None

# ========================================
# 6. RSS 전용 채널 데이터 수집
# ========================================
def get_channel_data_rss_only(channel_url, row_data, worksheet, row_number):
    """
    RSS 피드만 사용 - API 호출 제거
    ✅ RSS에서 영상 정보 추출
    ✅ 기존 시트 데이터 보존
    """
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
        # ✅ Step 1: 기존 시트 데이터 보존
        if len(row_data) >= COL_CHANNEL_NAME:
            result['channel_name'] = str(row_data[COL_CHANNEL_NAME - 1]).strip()
        if len(row_data) >= COL_HANDLE:
            result['handle'] = str(row_data[COL_HANDLE - 1]).strip()
        if len(row_data) >= COL_COUNTRY:
            country = str(row_data[COL_COUNTRY - 1]).strip()
            result['country'] = country if country else '한국'
        if len(row_data) >= COL_SUBSCRIBERS:
            try:
                result['subscribers'] = int(str(row_data[COL_SUBSCRIBERS - 1]))
            except:
                pass
        if len(row_data) >= COL_VIDEO_COUNT:
            try:
                result['video_count'] = int(str(row_data[COL_VIDEO_COUNT - 1]))
            except:
                pass
        if len(row_data) >= COL_TOTAL_VIEWS:
            try:
                result['total_views'] = int(str(row_data[COL_TOTAL_VIEWS - 1]))
            except:
                pass

        # ✅ Step 2: channel_id 추출 (기존 저장된 값 우선)
        existing_channel_id = ''
        if len(row_data) >= COL_CHANNEL_ID:
            existing_channel_id = str(row_data[COL_CHANNEL_ID - 1]).strip()

        channel_id = existing_channel_id
        
        if not channel_id:
            print(f"  📍 channel_id 추출 중...")
            channel_id = extract_channel_id_from_url(channel_url, row_data=row_data)
            
            if not channel_id:
                print(f"  ⚠️  channel_id를 추출할 수 없습니다")
                return result
            
            # channel_id 저장
            try:
                cell_list = [gspread.Cell(row_number, COL_CHANNEL_ID, channel_id)]
                worksheet.update_cells(cell_list)
                print(f"  ✅ channel_id 저장: {channel_id}")
                time.sleep(1)
            except Exception as e:
                print(f"  ⚠️  channel_id 저장 실패: {e}")
        else:
            print(f"  ✓ 기존 channel_id 사용: {channel_id}")
        
        result['channel_id'] = channel_id

        # ✅ Step 3: RSS 피드에서 모든 데이터 추출 (API 호출 없음!)
        rss_videos = parse_rss_feed_robust(channel_id, max_videos=30)
        
        if not rss_videos:
            print(f"  ⚠️  RSS 피드 데이터 없음")
            return result

        # ✅ Step 4: 날짜 계산
        dates = []
        for video in rss_videos:
            if video.get('published_at'):
                dates.append(video['published_at'])
        
        if dates:
            latest_date = max(dates)
            first_date = min(dates)
            
            result['latest_upload'] = latest_date.strftime('%Y-%m-%d')
            result['first_upload'] = first_date.strftime('%Y-%m-%d')
            result['operation_days'] = max(0, (latest_date - first_date).days)
            
            print(f"  ✅ 최초업로드: {result['first_upload']}")
            print(f"  ✅ 최근업로드: {result['latest_upload']}")
            print(f"  ✅ 운영기간: {result['operation_days']}일")

        print(f"  ✅ RSS 기반 데이터 추출 완료 (API 호출: 0회)")
        return result

    except Exception as e:
        print(f"  ❌ 데이터 수집 실패: {e}")
        traceback.print_exc()
        return result

# ========================================
# 7. 배치 업데이트 (20행씩)
# ========================================
def build_cell_list(row_num, data_dict, manual_values, row_data):
    """행 데이터를 셀 리스트로 변환"""
    cell_list = []
    
    try:
        # 기존 데이터 읽기
        existing_url = row_data[COL_URL - 1] if len(row_data) >= COL_URL else ''
        existing_video_count = str(row_data[COL_VIDEO_COUNT - 1]).strip() if len(row_data) >= COL_VIDEO_COUNT else ''
        existing_total_views = str(row_data[COL_TOTAL_VIEWS - 1]).strip() if len(row_data) >= COL_TOTAL_VIEWS else ''
        
        # 국가(D열) 처리 로직
        existing_country = str(row_data[COL_COUNTRY - 1]).strip() if len(row_data) >= COL_COUNTRY else ''
        api_country = data_dict.get('country', '한국')
        
        # 국가 결정 로직
        if existing_country:
            if len(existing_country) == 2 and existing_country.upper() in COUNTRY_MAP:
                final_country = COUNTRY_MAP[existing_country.upper()]
            else:
                final_country = existing_country
        else:
            final_country = api_country
        
        # 업데이트할 컬럼 정의
        updates = {
            COL_CHANNEL_NAME: data_dict.get('channel_name', ''),
            COL_URL: existing_url,
            COL_HANDLE: data_dict.get('handle', ''),
            COL_COUNTRY: final_country,
            COL_SUBSCRIBERS: int(data_dict.get('subscribers', 0)),
            COL_VIDEO_COUNT: int(data_dict.get('video_count', 0)) if not existing_video_count else int(existing_video_count),
            COL_TOTAL_VIEWS: int(data_dict.get('total_views', 0)) if not existing_total_views else int(existing_total_views),
            COL_FIRST_UPLOAD: data_dict.get('first_upload', ''),
            COL_LATEST_UPLOAD: data_dict.get('latest_upload', ''),
            COL_COLLECT_DATE: data_dict.get('collect_date', ''),
            COL_VIEWS_5_TOTAL: int(data_dict.get('views_5', 0)),
            COL_VIEWS_10_TOTAL: int(data_dict.get('views_10', 0)),
            COL_VIEWS_20_TOTAL: int(data_dict.get('views_20', 0)),
            COL_VIEWS_30_TOTAL: int(data_dict.get('views_30', 0)),
            COL_OPERATION_DAYS: int(data_dict.get('operation_days', 0)),
            COL_COUNT_5D: int(data_dict.get('count_5d', 0)),
            COL_COUNT_10D: int(data_dict.get('count_10d', 0)),
            COL_CHANNEL_ID: data_dict.get('channel_id', ''),
            COL_VIEWS_5D: int(data_dict.get('views_5d', 0)),
            COL_VIEWS_10D: int(data_dict.get('views_10d', 0)),
            COL_VIEWS_15D: int(data_dict.get('views_15d', 0)),
            COL_YT_CATEGORY: data_dict.get('yt_category', '미분류'),
            COL_CHANNEL_THUMBNAIL: data_dict.get('channel_thumbnail', ''),
        }
        
        # 셀 추가
        for col_idx, value in updates.items():
            cell_list.append(gspread.Cell(row_num, col_idx, value))
        
        # 동영상 링크 (썸네일)
        video_links = data_dict.get('video_links', [''] * 5)
        for i, col_idx in enumerate(COL_VIDEO_LINKS):
            if i < len(video_links) and video_links[i]:
                cell_list.append(gspread.Cell(row_num, col_idx, video_links[i]))
        
        # 수동 입력 컬럼 (기존 값 보존)
        for col, value in manual_values.items():
            if value:
                cell_list.append(gspread.Cell(row_num, col, value))
        
        return cell_list
    
    except ValueError as e:
        print(f"❌ 값 변환 실패 (Row {row_num}): {e}")
        return []
    except Exception as e:
        print(f"❌ 셀 리스트 생성 실패 (Row {row_num}): {e}")
        traceback.print_exc()
        return []

# ========================================
# 8. 메인 실행
# ========================================
def main():
    """메인 실행 함수"""
    print("="*60)
    print("📂 YouTube 채널 분석기 v2 - RSS 전용 모드")
    print("✅ API 호출 완전 제거 (쿼터 무제한)")
    print("✅ 강화된 RSS 파싱 (User-Agent + 재시도)")
    print("✅ yt-dlp 백업 방식")
    print("="*60)

    try:
        print(f"\n📊 '{SHEET_NAME}' 시트 연결 중...")
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            SERVICE_ACCOUNT_FILE, scope)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(DATA_TAB_NAME)

        print("✅ 시트 연결 완료\n")

        # 범위 설정
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

        # 시트 데이터 일괄 로드
        print("📥 시트 데이터 일괄 로드 중...")
        all_sheet_data = worksheet.get_all_values()
        print(f"✅ {len(all_sheet_data)}행 데이터 로드 완료\n")

        print("="*60)
        print("🚀 채널 분석 시작 (RSS 전용 모드)")
        print("="*60)

        success_count = 0
        fail_count = 0
        start_time = time.time()
        
        batch_cells = []
        batch_rows_count = 0

        for row_num in range(start_row, end_row + 1):
            print(f"\n{'='*60}")
            print(f"🔍 [{row_num - start_row + 1}/{end_row - start_row + 1}] 처리 중...")
            print(f"{'='*60}")

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

                # 수동 입력 컬럼 보존
                manual_values = preserve_manual_columns_batch(all_sheet_data, row_num)

                # ✅ RSS 전용 모드로 데이터 수집 (API 호출 0회)
                data = get_channel_data_rss_only(url, row_data, worksheet, row_num)

                if not data:
                    print(f"❌ Row {row_num}: 데이터 수집 실패")
                    fail_count += 1
                    continue

                cells = build_cell_list(row_num, data, manual_values, row_data)
                batch_cells.extend(cells)
                batch_rows_count += 1
                success_count += 1
                
                print(f"✅ Row {row_num} 준비 완료 ({len(cells)}개 셀)")

                # 배치 업데이트
                if batch_rows_count >= BATCH_SIZE or row_num == end_row:
                    if batch_cells:
                        print(f"\n📤 배치 업데이트 실행: {batch_rows_count}행, {len(batch_cells)}개 셀")
                        worksheet.update_cells(batch_cells)
                        print(f"✅ 배치 업데이트 완료!")
                        batch_cells = []
                        batch_rows_count = 0
                        
                        print(f"💤 2초 대기...")
                        time.sleep(2)
                
                time.sleep(1)  # 최소 대기 시간

            except Exception as e:
                print(f"❌ Row {row_num} 처리 중 오류: {e}")
                traceback.print_exc()
                fail_count += 1
                time.sleep(3)
                continue

        if batch_cells:
            print(f"\n📤 최종 배치 업데이트: {batch_rows_count}행, {len(batch_cells)}개 셀")
            worksheet.update_cells(batch_cells)
            print(f"✅ 최종 배치 업데이트 완료!")

        elapsed_time = time.time() - start_time
        print("\n" + "="*60)
        print("📊 최종 결과")
        print("="*60)
        print(f"✅ 성공: {success_count}개")
        print(f"❌ 실패: {fail_count}개")
        print(f"⏱️  소요 시간: {elapsed_time / 60:.1f}분")
        if (success_count + fail_count) > 0:
            print(f"⚡ 평균 속도: {elapsed_time / (success_count + fail_count):.1f}초/채널")
        print("="*60)

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        traceback.print_exc()

# ========================================
# 9. 실행
# ========================================
if __name__ == '__main__':
    main()
