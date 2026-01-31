# ========================================
# YouTube 채널 분석기 v2 - GitHub Actions 버전
# RSS + YouTube API 하이브리드 방식
# ========================================

# ========================================
# 1. 라이브러리 임포트
# ========================================
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
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

# Google Sheets 설정 (환경변수 우선, 없으면 기본값)
SHEET_NAME = os.environ.get('SHEET_NAME', '유튜브보물창고_테스트')
API_TAB_NAME = os.environ.get('API_TAB_NAME', 'API_키_관리')
DATA_TAB_NAME = os.environ.get('DATA_TAB_NAME', '데이터2')

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
COL_VIDEO_LINKS = [29, 30, 31, 32, 33]  # AC~AG: 영상1~5

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

def get_video_url(video_id):
    """영상 ID로 YouTube URL 생성"""
    if not video_id:
        return ''
    return f"https://www.youtube.com/watch?v={video_id}"

def get_video_urls(video_ids, max_count=5):
    """상위 5개 영상의 YouTube URL 리스트 반환"""
    urls = []
    for vid in video_ids[:max_count]:
        urls.append(get_video_url(vid))
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
# 4. API 키 매니저
# ========================================
class YouTubeAPIKeyManager:
    """YouTube API 키 관리 및 쿼터 추적"""

    def __init__(self, service_account_file, sheet_name, api_tab_name):
        self.service_account_file = service_account_file
        self.sheet_name = sheet_name
        self.api_tab_name = api_tab_name

        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            service_account_file, scope)
        self.gc = gspread.authorize(creds)

        try:
            spreadsheet = self.gc.open(sheet_name)
            self.api_sheet = spreadsheet.worksheet(api_tab_name)
            print(f"✅ '{api_tab_name}' 시트 연결 성공")
        except Exception as e:
            print(f"❌ '{api_tab_name}' 시트 연결 실패: {e}")
            raise

        self.api_keys = []
        self.quota_status = {}
        self.current_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        self.load_keys_from_sheet()
        print(f"✅ API 키 {len(self.api_keys)}개 로드 완료\n")

    def load_keys_from_sheet(self):
        """시트에서 API 키 및 사용량 로드"""
        try:
            all_values = self.api_sheet.get_all_values()

            if len(all_values) < 4:
                print("⚠️  API 키 데이터가 없습니다")
                return

            headers = all_values[2]

            try:
                idx_name = headers.index('키 이름')
                idx_key = headers.index('API 키')
                idx_active = headers.index('활성화')
                idx_quota = headers.index('할당량 (전체)')
                idx_used = headers.index('사용량')
            except ValueError as e:
                print(f"❌ 필수 컬럼을 찾을 수 없습니다: {e}")
                print(f"   실제 헤더: {headers}")
                raise

            for row_idx, row in enumerate(all_values[3:], start=4):
                if len(row) <= max(idx_name, idx_key, idx_active):
                    continue

                key_name = row[idx_name].strip()
                api_key = row[idx_key].strip()
                active = row[idx_active]

                is_active = str(active).upper() in ['TRUE', 'YES', 'Y', 'O', '활성', '사용']

                if not key_name or not api_key or not is_active:
                    continue

                try:
                    total_quota = int(row[idx_quota]) if idx_quota < len(row) and row[idx_quota] else 10000
                    used_quota = int(row[idx_used]) if idx_used < len(row) and row[idx_used] else 0
                except:
                    total_quota = 10000
                    used_quota = 0

                self.api_keys.append({
                    'name': key_name,
                    'key': api_key,
                    'row': row_idx,
                    'active': True
                })

                self.quota_status[key_name] = {
                    'total': total_quota,
                    'used': used_quota,
                    'remaining': total_quota - used_quota,
                    'errors': 0,
                    'last_reset': self.current_date,
                    'session_used': 0
                }

                print(f"  ✓ {key_name}: {api_key[:20]}... (할당량: {total_quota}, 사용: {used_quota})")

        except Exception as e:
            print(f"❌ API 키 로드 실패: {e}")
            traceback.print_exc()
            raise

    def get_key_for_row(self, row_number, required_quota=110):
        """특정 행에 할당된 API 키 반환"""
        if not self.api_keys:
            raise Exception("❌ 사용 가능한 API 키가 없습니다")

        key_idx = (row_number - 4) % len(self.api_keys)
        selected_key = self.api_keys[key_idx]
        key_name = selected_key['name']

        status = self.quota_status[key_name]
        if status['remaining'] >= required_quota:
            return selected_key

        for backup_key in self.api_keys:
            backup_name = backup_key['name']
            backup_status = self.quota_status[backup_name]
            if backup_status['remaining'] >= required_quota:
                return backup_key

        raise Exception(f"❌ 모든 API 키의 할당량이 부족합니다")

    def update_quota_used(self, key_name, units):
        """API 키 사용량 업데이트"""
        if key_name in self.quota_status:
            self.quota_status[key_name]['used'] += units
            self.quota_status[key_name]['remaining'] -= units
            self.quota_status[key_name]['session_used'] += units

    def sync_to_sheet(self):
        """메모리의 사용량을 시트에 동기화 (배치 업데이트)"""
        try:
            print("\n  🔄 API 키 사용량 시트 동기화 중...")
            
            all_values = self.api_sheet.get_all_values()
            headers = all_values[2]
            
            idx_name = headers.index('키 이름')
            idx_used = headers.index('사용량') + 1
            idx_remaining = headers.index('남은량') + 1
            idx_rate = headers.index('사용률 (%)') + 1
            idx_last_used = headers.index('마지막 사용') + 1
            
            cell_list = []
            
            for key_info in self.api_keys:
                key_name = key_info['name']
                row_num = key_info['row']
                
                if key_name not in self.quota_status:
                    continue
                
                status = self.quota_status[key_name]
                
                cell_list.append(gspread.Cell(row_num, idx_used, status['used']))
                cell_list.append(gspread.Cell(row_num, idx_remaining, status['remaining']))
                
                usage_rate = (status['used'] / status['total'] * 100) if status['total'] > 0 else 0
                cell_list.append(gspread.Cell(row_num, idx_rate, f"{usage_rate:.2f}%"))
                
                if status['session_used'] > 0:
                    last_used = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                    cell_list.append(gspread.Cell(row_num, idx_last_used, last_used))
            
            if cell_list:
                self.api_sheet.update_cells(cell_list)
                print(f"  ✅ API 키 사용량 시트 동기화 완료 ({len(cell_list)}개 셀)")
            
        except Exception as e:
            print(f"  ⚠️  시트 동기화 실패: {e}")
            traceback.print_exc()

    def print_status(self):
        """현재 API 키 상태 출력"""
        print("\n" + "="*80)
        print("📊 API 키 할당량 현황")
        print("="*80)

        total_used = 0
        total_remaining = 0

        for key_name, status in self.quota_status.items():
            total_used += status['used']
            total_remaining += status['remaining']
            
            print(f"  {key_name:15s} | "
                  f"전체: {status['total']:6,d} | "
                  f"사용: {status['used']:6,d} | "
                  f"남음: {status['remaining']:6,d} | "
                  f"세션: {status['session_used']:6,d}")

        print("-"*80)
        print(f"  {'전체 합계':15s} | "
              f"사용: {total_used:6,d} | "
              f"남음: {total_remaining:6,d}")
        print("="*80 + "\n")

# ========================================
# 5. RSS 피드 파싱
# ========================================
def parse_rss_feed(channel_id, max_videos=15):
    """YouTube RSS 피드에서 최근 영상 정보 추출"""
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"

    try:
        feed = feedparser.parse(rss_url)

        if not feed.entries:
            return []

        videos = []
        for entry in feed.entries[:max_videos]:
            video_id = entry.yt_videoid if hasattr(entry, 'yt_videoid') else None
            if not video_id and 'id' in entry:
                video_id = entry.id.split(':')[-1]

            published_str = entry.published if hasattr(entry, 'published') else None
            published_at = None
            if published_str:
                try:
                    from email.utils import parsedate_to_datetime
                    published_at = parsedate_to_datetime(published_str)
                except:
                    pass

            videos.append({
                'video_id': video_id,
                'title': entry.title if hasattr(entry, 'title') else '',
                'published_at': published_at
            })

        return videos

    except Exception as e:
        print(f"  ⚠️  RSS 피드 파싱 실패: {e}")
        return []

# ========================================
# 6. 채널 ID 추출
# ========================================
def extract_channel_id_from_url(channel_url, api_manager, row_number, row_data=None):
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
            except:
                handle_decoded = handle_from_sheet

            try:
                api_key_info = api_manager.get_key_for_row(row_number, required_quota=1)
                api_key = api_key_info['key']
                key_name = api_key_info['name']

                youtube = build('youtube', 'v3', developerKey=api_key)

                channel_response = youtube.channels().list(
                    part='id',
                    forHandle=handle_decoded,
                    maxResults=1
                ).execute()

                api_manager.update_quota_used(key_name, 1)

                if channel_response['items']:
                    channel_id = channel_response['items'][0]['id']
                    print(f"  ✓ 채널 ID 추출 성공 (C열 + forHandle): {channel_id}")
                    return channel_id
            except Exception as e:
                print(f"  ⚠️  C열 핸들로 forHandle 실패: {e}")

    decoded_url = urllib.parse.unquote(channel_url)
    handle_match = re.search(r'@([^/\s?]+)', decoded_url)
    
    if not handle_match:
        print(f"  ⚠️  URL에서 핸들을 추출할 수 없음")
        return None

    handle = handle_match.group(1)
    print(f"  📍 URL에서 핸들 추출: @{handle}")

    try:
        api_key_info = api_manager.get_key_for_row(row_number, required_quota=1)
        api_key = api_key_info['key']
        key_name = api_key_info['name']

        youtube = build('youtube', 'v3', developerKey=api_key)

        channel_response = youtube.channels().list(
            part='id',
            forHandle=handle,
            maxResults=1
        ).execute()

        api_manager.update_quota_used(key_name, 1)

        if channel_response['items']:
            channel_id = channel_response['items'][0]['id']
            print(f"  ✓ 채널 ID 추출 성공 (URL + forHandle): {channel_id}")
            return channel_id
    except Exception as e:
        print(f"  ⚠️  forHandle 실패: {e}")

    try:
        import requests
        clean_url = channel_url.split('/shorts')[0].split('/videos')[0].split('/streams')[0]
        response = requests.get(clean_url, timeout=10)

        patterns = [
            r'"channelId":"([^"]+)"',
            r'"externalId":"([^"]+)"',
        ]

        for pattern in patterns:
            match = re.search(pattern, response.text)
            if match:
                channel_id = match.group(1)
                print(f"  ✓ 채널 ID 추출 성공 (웹 스크래핑): {channel_id}")
                return channel_id
    except Exception as e:
        print(f"  ⚠️  웹 스크래핑 실패: {e}")

    return None

def extract_channel_id_ytdlp(url):
    """yt-dlp로 채널 ID 추출"""
    try:
        result = subprocess.run(
            ['yt-dlp', '--dump-json', '--playlist-items', '1', url],
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
# 7. 메인 채널 데이터 수집
# ========================================
def get_channel_data_hybrid(channel_url, api_manager, row_number, row_data, worksheet):
    """RSS + API 하이브리드 방식으로 채널 데이터 수집"""
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
                api_manager,
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

        print(f"  📡 RSS 피드 수집 중...")
        rss_videos = parse_rss_feed(channel_id, max_videos=15)
        print(f"  ✓ RSS에서 {len(rss_videos)}개 영상 수집")

        api_key_info = api_manager.get_key_for_row(row_number, required_quota=3)
        api_key = api_key_info['key']
        key_name = api_key_info['name']

        youtube = build('youtube', 'v3', developerKey=api_key)

        channel_response = youtube.channels().list(
            part='snippet,statistics,contentDetails',
            id=channel_id
        ).execute()

        api_manager.update_quota_used(key_name, 1)

        if not channel_response['items']:
            print(f"  ❌ 채널 정보를 찾을 수 없습니다")
            return None

        channel_info = channel_response['items'][0]
        snippet = channel_info['snippet']
        statistics = channel_info['statistics']

        result['channel_name'] = snippet.get('title', '')
        result['handle'] = snippet.get('customUrl', '')
        
        country_code = snippet.get('country', '').strip()
        if not country_code:
            result['country'] = '한국'
            print(f"  ℹ️  국가 정보 없음 → '한국'으로 설정")
        else:
            result['country'] = get_country_name(country_code)
            print(f"  ℹ️  국가: {result['country']} ({country_code})")

        result['subscribers'] = int(statistics.get('subscriberCount', 0))
        result['video_count'] = int(statistics.get('videoCount', 0))
        result['total_views'] = int(statistics.get('viewCount', 0))

        print(f"  ✓ 채널: {result['channel_name']}")
        print(f"  ✓ 구독자: {result['subscribers']:,} | 영상: {result['video_count']:,}")

        uploads_playlist_id = channel_info['contentDetails']['relatedPlaylists']['uploads']

        playlist_response = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=uploads_playlist_id,
            maxResults=30
        ).execute()

        api_manager.update_quota_used(key_name, 1)

        api_videos = []
        for item in playlist_response['items'][15:30]:
            video_id = item['contentDetails']['videoId']
            api_videos.append(video_id)

        print(f"  ✓ API에서 {len(api_videos)}개 영상 수집 (16~30번째)")

        all_video_ids = [v['video_id'] for v in rss_videos if v['video_id']] + api_videos
        all_video_ids = all_video_ids[:30]

        if not all_video_ids:
            print(f"  ⚠️  수집된 영상이 없습니다")
            return result

        videos_response = youtube.videos().list(
            part='statistics,snippet',
            id=','.join(all_video_ids)
        ).execute()

        api_manager.update_quota_used(key_name, 1)

        view_map = {}
        for video in videos_response['items']:
            video_id = video['id']
            view_count = int(video['statistics'].get('viewCount', 0))
            published_str = video['snippet'].get('publishedAt', '')

            published_at = None
            if published_str:
                try:
                    published_at = datetime.fromisoformat(published_str.replace('Z', '+00:00'))
                except:
                    pass

            view_map[video_id] = (view_count, published_at)

        if videos_response['items']:
            first_category_id = videos_response['items'][0]['snippet'].get('categoryId', '')
            result['yt_category'] = get_category_name(first_category_id)

        result['video_links'] = get_video_urls([v['id'] for v in videos_response['items']], max_count=5)

        views_list = []
        for video_id in all_video_ids:
            if video_id in view_map:
                views_list.append(view_map[video_id][0])

        result['views_5'] = sum(views_list[:5])
        result['views_10'] = sum(views_list[:10])
        result['views_20'] = sum(views_list[:20])
        result['views_30'] = sum(views_list[:30])

        now = datetime.now(timezone.utc)

        views_5d_list = []
        views_10d_list = []
        views_15d_list = []

        print(f"  📅 날짜 기준 조회수 계산 (기준: {now.strftime('%Y-%m-%d %H:%M UTC')})")

        for video_id in all_video_ids:
            if video_id not in view_map:
                continue

            view_count, published_at = view_map[video_id]

            if not published_at:
                continue

            days_ago = (now - published_at).days

            if days_ago <= 15:
                print(f"    📅 {video_id}: {days_ago}일 전 | {view_count:,}회")

            if days_ago <= 5:
                views_5d_list.append(view_count)
            if days_ago <= 10:
                views_10d_list.append(view_count)
            if days_ago <= 15:
                views_15d_list.append(view_count)

        result['views_5d'] = sum(views_5d_list)
        result['views_10d'] = sum(views_10d_list)
        result['views_15d'] = sum(views_15d_list)
        result['count_5d'] = len(views_5d_list)
        result['count_10d'] = len(views_10d_list)

        print(f"  ✅ 5일: {result['views_5d']:,}회 ({result['count_5d']}개)")
        print(f"  ✅ 10일: {result['views_10d']:,}회 ({result['count_10d']}개)")
        print(f"  ✅ 15일: {result['views_15d']:,}회")

        dates = []
        for video_id in all_video_ids:
            if video_id in view_map and view_map[video_id][1]:
                dates.append(view_map[video_id][1])

        if dates:
            result['latest_upload'] = max(dates).strftime('%Y-%m-%d')
            result['first_upload'] = min(dates).strftime('%Y-%m-%d')
            first_date = min(dates)
            result['operation_days'] = (now - first_date).days

        return result

    except Exception as e:
        print(f"  ❌ 데이터 수집 실패: {e}")
        traceback.print_exc()
        return None

# ========================================
# 8. 수동 입력 컬럼 보존
# ========================================
def preserve_manual_columns(worksheet, row_num):
    """수동 입력 컬럼의 기존 값 읽기"""
    try:
        manual_values = {}
        for col in MANUAL_INPUT_COLUMNS:
            cell_value = worksheet.cell(row_num, col).value
            manual_values[col] = cell_value if cell_value else ''
        return manual_values
    except Exception as e:
        print(f"⚠️ 수동 컬럼 읽기 실패: {e}")
        return {col: '' for col in MANUAL_INPUT_COLUMNS}

# ========================================
# 9. 배치 업데이트
# ========================================
def update_row_batch(worksheet, row_num, data_dict, manual_values):
    """33개 셀을 한 번에 업데이트 (B열 URL은 보존)"""
    try:
        existing_url = worksheet.cell(row_num, COL_URL).value or ''
        
        row_data = [''] * 33

        row_data[COL_CHANNEL_NAME - 1] = data_dict.get('channel_name', '')
        row_data[COL_URL - 1] = existing_url
        row_data[COL_HANDLE - 1] = data_dict.get('handle', '')
        row_data[COL_COUNTRY - 1] = data_dict.get('country', '')
        row_data[COL_SUBSCRIBERS - 1] = data_dict.get('subscribers', 0)
        row_data[COL_VIDEO_COUNT - 1] = data_dict.get('video_count', 0)
        row_data[COL_TOTAL_VIEWS - 1] = data_dict.get('total_views', 0)
        row_data[COL_FIRST_UPLOAD - 1] = data_dict.get('first_upload', '')
        row_data[COL_LATEST_UPLOAD - 1] = data_dict.get('latest_upload', '')
        row_data[COL_COLLECT_DATE - 1] = data_dict.get('collect_date', '')
        row_data[COL_VIEWS_5_TOTAL - 1] = data_dict.get('views_5', 0)
        row_data[COL_VIEWS_10_TOTAL - 1] = data_dict.get('views_10', 0)
        row_data[COL_VIEWS_20_TOTAL - 1] = data_dict.get('views_20', 0)
        row_data[COL_VIEWS_30_TOTAL - 1] = data_dict.get('views_30', 0)
        row_data[COL_OPERATION_DAYS - 1] = data_dict.get('operation_days', 0)
        row_data[COL_COUNT_5D - 1] = data_dict.get('count_5d', 0)
        row_data[COL_COUNT_10D - 1] = data_dict.get('count_10d', 0)
        row_data[COL_CHANNEL_ID - 1] = data_dict.get('channel_id', '')
        row_data[COL_VIEWS_5D - 1] = data_dict.get('views_5d', 0)
        row_data[COL_VIEWS_10D - 1] = data_dict.get('views_10d', 0)
        row_data[COL_VIEWS_15D - 1] = data_dict.get('views_15d', 0)
        row_data[COL_YT_CATEGORY - 1] = data_dict.get('yt_category', '미분류')

        video_links = data_dict.get('video_links', [''] * 5)
        for i, col_idx in enumerate(COL_VIDEO_LINKS):
            row_data[col_idx - 1] = video_links[i]

        for col in MANUAL_INPUT_COLUMNS:
            row_data[col - 1] = manual_values.get(col, '')

        range_str = f'A{row_num}:AG{row_num}'
        worksheet.update(range_str, [row_data])

        print(f"✅ Row {row_num} 배치 업데이트 완료 (B열 URL 보존)")
        return True

    except Exception as e:
        print(f"❌ 배치 업데이트 실패: {e}")
        traceback.print_exc()
        return False

# ========================================
# 10. 메인 실행
# ========================================
def main():
    """메인 실행 함수"""
    print("=" * 60)
    print("📂 YouTube 채널 분석기 v2 - GitHub Actions 버전")
    print("=" * 60)

    try:
        print("\n📋 API 키 매니저 초기화 중...")
        api_manager = YouTubeAPIKeyManager(
            SERVICE_ACCOUNT_FILE,
            SHEET_NAME,
            API_TAB_NAME
        )

        if not api_manager.api_keys:
            print("❌ API 키가 없습니다. 'API_키_관리' 탭을 확인하세요.")
            return

        print(f"📊 '{SHEET_NAME}' 시트 연결 중...")
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            SERVICE_ACCOUNT_FILE, scope)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(DATA_TAB_NAME)

        print("✅ 시트 연결 완료\n")

        print("=" * 60)
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

        print("\n" + "=" * 60)
        print("🚀 채널 분석 시작")
        print("=" * 60)

        success_count = 0
        fail_count = 0
        start_time = time.time()

        for row_num in range(start_row, end_row + 1):
            print(f"\n{'='*60}")
            print(f"🔍 [{row_num - start_row + 1}/{end_row - start_row + 1}] 처리 중...")
            print(f"{'='*60}")

            try:
                row_data = worksheet.row_values(row_num)
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

                manual_values = preserve_manual_columns(worksheet, row_num)

                data = get_channel_data_hybrid(url, api_manager, row_num, row_data, worksheet)

                if not data:
                    print(f"❌ Row {row_num}: 데이터 수집 실패")
                    fail_count += 1
                    continue

                if update_row_batch(worksheet, row_num, data, manual_values):
                    success_count += 1
                    print(f"✅ Row {row_num} 완료!")
                else:
                    fail_count += 1

                if (row_num - start_row + 1) % 5 == 0:
                    api_manager.sync_to_sheet()
                    api_manager.print_status()
                    print(f"💤 30초 대기...")
                    time.sleep(30)
                else:
                    time.sleep(3)

            except Exception as e:
                print(f"❌ Row {row_num} 처리 중 오류: {e}")
                traceback.print_exc()
                fail_count += 1
                continue

        elapsed_time = time.time() - start_time
        print("\n" + "=" * 60)
        print("📊 최종 결과")
        print("=" * 60)
        print(f"✅ 성공: {success_count}개")
        print(f"❌ 실패: {fail_count}개")
        print(f"⏱️  소요 시간: {elapsed_time / 60:.1f}분")
        if (success_count + fail_count) > 0:
            print(f"⚡ 평균 속도: {elapsed_time / (success_count + fail_count):.1f}초/채널")
        print("=" * 60)

        api_manager.sync_to_sheet()
        api_manager.print_status()

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        traceback.print_exc()

# ========================================
# 11. 실행
# ========================================
if __name__ == '__main__':
    main()
