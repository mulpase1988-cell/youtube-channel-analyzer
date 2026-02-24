# ========================================
# YouTube 채널 분석기 v5 - ID 추출 강화 버전
# /shorts URL 및 @핸들 주소 처리 로직 개선
# channelId, browseId, externalId 다중 검색
# ========================================

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta
import time
import re
import os
import json
import tempfile
import traceback
import requests
import scrapetube
import feedparser
import random

# ========================================
# 1. 설정 변수
# ========================================

SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT')
if not SERVICE_ACCOUNT_JSON:
    raise Exception("❌ GOOGLE_SERVICE_ACCOUNT 환경변수가 설정되지 않았습니다")

with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
    f.write(SERVICE_ACCOUNT_JSON)
    SERVICE_ACCOUNT_FILE = f.name

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
    'PH': '필리핀', 'CN': '중국', 'SG': '싱가포르', 'MY': '말레이시아'
}

# ========================================
# 2. 강력한 ID 추출 함수 (핵심 수정)
# ========================================

def get_channel_id_robust(url):
    """
    다양한 URL 패턴에서 채널 ID를 강력하게 추출
    1. URL 자체에 ID가 있는 경우
    2. HTML 파싱 (Home, About 탭 순차 검색)
    3. browseId, channelId, externalId 등 다양한 키워드 검색
    """
    
    # 1. URL 정리 (/shorts, /videos 등 제거)
    clean_url = url.split('/shorts')[0].split('/videos')[0].split('/streams')[0].split('?')[0]
    
    # 이미 ID 형태인 경우 (channel/UC...)
    if '/channel/' in clean_url:
        return clean_url.split('/channel/')[-1].split('/')[0]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://www.google.com/',
        'Cookie': 'CONSENT=YES+KR.ko+20150622-23-0; NIS=1;' # 쿠키 추가로 차단 확률 감소
    }

    # 탐색할 URL 후보 (홈 -> 정보 탭)
    target_urls = [clean_url, f"{clean_url}/about"]

    # 검색할 정규식 패턴들 (우선순위 순)
    patterns = [
        r'"channelId":"(UC[\w-]{21,})"',      # 일반적인 패턴
        r'"browseId":"(UC[\w-]{21,})"',       # @핸들 접속 시 주로 나오는 패턴
        r'"externalId":"(UC[\w-]{21,})"',     # 구형 패턴
        r'itemprop="channelId" content="(UC[\w-]{21,})"', # 메타 태그
        r'data-channel-id="(UC[\w-]{21,})"'   # 일부 HTML 속성
    ]

    for target in target_urls:
        try:
            # print(f"      🕵️ URL 탐색 중: {target}")
            res = requests.get(target, headers=headers, timeout=10)
            
            if res.status_code != 200:
                continue

            text = res.text
            
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    found_id = match.group(1)
                    # UC로 시작하고 길이가 24자인지 검증 (가끔 이상한 값이 잡힐 수 있음)
                    if found_id.startswith('UC') and len(found_id) == 24:
                        return found_id
        except Exception:
            pass
            
    return None

# ========================================
# 3. 기타 파싱 헬퍼 함수
# ========================================

def get_channel_stats_html(channel_url):
    """HTML에서 구독자, 채널명, 썸네일 추출"""
    stats = {
        'subscribers': 0,
        'channel_name': '',
        'channel_thumbnail': '',
        'handle': ''
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }
    
    try:
        # /shorts가 있으면 제거하고 요청
        clean_url = channel_url.split('/shorts')[0]
        res = requests.get(clean_url, headers=headers, timeout=10)
        text = res.text
        
        # 채널명
        title_match = re.search(r'<meta property="og:title" content="([^"]+)">', text)
        if title_match:
            stats['channel_name'] = title_match.group(1)
            
        # 썸네일
        thumb_match = re.search(r'<meta property="og:image" content="([^"]+)">', text)
        if thumb_match:
            stats['channel_thumbnail'] = thumb_match.group(1)
            
        # 핸들
        if '@' in channel_url:
            stats['handle'] = '@' + channel_url.split('@')[1].split('/')[0]
        
        # 구독자 (텍스트 패턴)
        sub_match = re.search(r'"subscriberCountText":\{"accessibility":\{"accessibilityData":\{"label":"([^"]+)"', text)
        if sub_match:
            stats['subscribers'] = parse_korean_count(sub_match.group(1))
        
    except Exception as e:
        print(f"  ⚠️ HTML 통계 파싱 에러: {e}")
        
    return stats

def parse_korean_count(text):
    """구독자 1.23만명 -> 12300 변환"""
    try:
        text = re.sub(r'[^0-9.천만억KMB]', '', text)
        multiplier = 1
        if '천' in text or 'K' in text: multiplier = 1000
        elif '만' in text: multiplier = 10000
        elif '억' in text or 'M' in text: multiplier = 100000000
        elif 'B' in text: multiplier = 1000000000
        
        text = text.replace('천','').replace('만','').replace('억','').replace('K','').replace('M','').replace('B','')
        return int(float(text) * multiplier)
    except:
        return 0

def get_exact_date_from_rss(channel_id):
    """RSS로 정확한 날짜 가져오기"""
    if not channel_id: return []
    try:
        rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        feed = feedparser.parse(rss_url)
        videos = []
        for entry in feed.entries:
            published = None
            if hasattr(entry, 'published_parsed'):
                published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            videos.append({
                'video_id': entry.yt_videoid,
                'published_at': published
            })
        return videos
    except:
        return []

# ========================================
# 4. 메인 데이터 수집 로직
# ========================================

def get_channel_data_v5(channel_url, row_num, row_data):
    """v5: ID 추출 강화 + Scrapetube + RSS"""
    
    result = {
        'channel_name': '', 'handle': '', 'country': '',
        'subscribers': 0, 'video_count': 0, 'total_views': 0,
        'first_upload': '', 'latest_upload': '',
        'collect_date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
        'views_5': 0, 'views_10': 0, 'views_20': 0, 'views_30': 0,
        'views_5d': 0, 'views_10d': 0, 'views_15d': 0,
        'count_5d': 0, 'count_10d': 0,
        'operation_days': 0,
        'channel_id': '', 'yt_category': '미분류',
        'channel_thumbnail': '',
        'video_links': ['', '', '', '', '']
    }
    
    # URL 정리
    clean_url = channel_url.split('/shorts')[0].split('/videos')[0].split('?')[0]
    print(f"  📡 데이터 수집 중... (URL: {clean_url})")

    # 1. 채널 ID 추출 (v5 강화 버전)
    channel_id = get_channel_id_robust(channel_url)
    
    if channel_id:
        result['channel_id'] = channel_id
        print(f"    ✓ 채널 ID 추출 성공: {channel_id}")
    else:
        # 시트에 이미 ID가 있는지 확인 (백업)
        if len(row_data) >= COL_CHANNEL_ID and str(row_data[COL_CHANNEL_ID-1]).startswith('UC'):
            channel_id = str(row_data[COL_CHANNEL_ID-1])
            result['channel_id'] = channel_id
            print(f"    ✓ 기존 시트 ID 사용: {channel_id}")
        else:
            print("    ❌ 채널 ID 추출 실패 (건너뜀)")
            return None

    # 2. RSS 데이터 (날짜 확인용)
    rss_videos = get_exact_date_from_rss(channel_id)
    rss_map = {v['video_id']: v for v in rss_videos}
    
    if rss_videos:
        result['latest_upload'] = rss_videos[0]['published_at'].strftime('%Y-%m-%d')

    # 3. HTML 통계 (구독자, 썸네일)
    html_stats = get_channel_stats_html(clean_url)
    result['subscribers'] = html_stats['subscribers']
    result['channel_thumbnail'] = html_stats['channel_thumbnail']
    if html_stats['channel_name']: result['channel_name'] = html_stats['channel_name']
    if html_stats['handle']: result['handle'] = html_stats['handle']

    # 4. Scrapetube (영상 목록)
    print("    ✓ 영상 목록 수집 중 (Scrapetube)...")
    combined_videos = []
    try:
        # ID로 조회하면 차단 확률이 낮음
        videos = scrapetube.get_channel(channel_id, limit=30)
        
        for v in videos:
            video_id = v['videoId']
            
            # 조회수
            view_count = 0
            if 'viewCountText' in v:
                view_text = v['viewCountText'].get('simpleText', '') or v['viewCountText'].get('accessibility', {}).get('accessibilityData', {}).get('label', '')
                view_count = parse_korean_count(view_text)
            
            # 썸네일
            thumb_url = ''
            if 'thumbnail' in v and 'thumbnails' in v['thumbnail']:
                thumb_url = v['thumbnail']['thumbnails'][-1]['url']
            
            # 날짜 (RSS 우선, 없으면 None)
            published_at = None
            if video_id in rss_map:
                published_at = rss_map[video_id]['published_at']
            
            combined_videos.append({
                'id': video_id, 'views': view_count,
                'thumbnail': thumb_url, 'published_at': published_at
            })
            
    except Exception as e:
        print(f"    ⚠️ 영상 목록 수집 실패: {e}")

    # 5. 통계 집계
    for i in range(min(5, len(combined_videos))):
        result['video_links'][i] = combined_videos[i]['thumbnail']
        
    views_list = [v['views'] for v in combined_videos]
    result['views_5'] = sum(views_list[:5])
    result['views_10'] = sum(views_list[:10])
    result['views_20'] = sum(views_list[:20])
    result['views_30'] = sum(views_list[:30])
    
    # 기간별 통계
    now = datetime.now(timezone.utc)
    views_5d_list = []
    views_10d_list = []
    views_15d_list = []
    dates = []
    
    for v in combined_videos:
        if v['published_at']:
            dates.append(v['published_at'])
            days_ago = (now - v['published_at']).days
            
            if days_ago <= 5: views_5d_list.append(v['views'])
            if days_ago <= 10: views_10d_list.append(v['views'])
            if days_ago <= 15: views_15d_list.append(v['views'])
            
    result['views_5d'] = sum(views_5d_list)
    result['views_10d'] = sum(views_10d_list)
    result['views_15d'] = sum(views_15d_list)
    result['count_5d'] = len(views_5d_list)
    result['count_10d'] = len(views_10d_list)
    
    # 운영기간 (T열)
    existing_first = str(row_data[COL_FIRST_UPLOAD-1]).strip() if len(row_data) >= COL_FIRST_UPLOAD else ''
    first_date_dt = None
    if existing_first:
        try:
            first_date_dt = datetime.strptime(existing_first, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        except: pass
            
    if dates:
        min_collected = min(dates)
        if first_date_dt is None or min_collected < first_date_dt:
            first_date_dt = min_collected
    
    if first_date_dt:
        result['first_upload'] = first_date_dt.strftime('%Y-%m-%d')
        if result['latest_upload']:
            last_date_dt = datetime.strptime(result['latest_upload'], '%Y-%m-%d').replace(tzinfo=timezone.utc)
            result['operation_days'] = (last_date_dt - first_date_dt).days

    print(f"    ✅ 완료: 구독자 {result['subscribers']:,} | 최근영상 {result['latest_upload']}")
    return result

# ========================================
# 5. 시트 입출력 및 실행
# ========================================

def preserve_manual_columns_batch(all_sheet_data, row_num):
    try:
        row_idx = row_num - 1
        if row_idx >= len(all_sheet_data): return {col: '' for col in MANUAL_INPUT_COLUMNS}
        row_data = all_sheet_data[row_idx]
        manual_values = {}
        for col in MANUAL_INPUT_COLUMNS:
            cell_value = row_data[col - 1] if len(row_data) >= col else ''
            manual_values[col] = cell_value if cell_value else ''
        return manual_values
    except: return {col: '' for col in MANUAL_INPUT_COLUMNS}

def build_cell_list(row_num, data_dict, manual_values, row_data):
    cell_list = []
    try:
        existing_url = row_data[COL_URL - 1] if len(row_data) >= COL_URL else ''
        existing_country = str(row_data[COL_COUNTRY - 1]).strip() if len(row_data) >= COL_COUNTRY else ''
        
        final_country = data_dict.get('country', '')
        if existing_country and existing_country.upper() in COUNTRY_MAP:
             final_country = COUNTRY_MAP[existing_country.upper()]
        elif existing_country:
             final_country = existing_country

        columns_data = [
            (COL_CHANNEL_NAME, data_dict.get('channel_name', '')),
            (COL_URL, existing_url),
            (COL_HANDLE, data_dict.get('handle', '')),
            (COL_COUNTRY, final_country),
            (COL_SUBSCRIBERS, data_dict.get('subscribers', 0)),
            (COL_VIDEO_COUNT, data_dict.get('video_count', 0)),
            (COL_TOTAL_VIEWS, data_dict.get('total_views', 0)),
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
            (COL_CHANNEL_THUMBNAIL, data_dict.get('channel_thumbnail', '')),
        ]
        for col_idx, value in columns_data:
            if value or value == 0: cell_list.append(gspread.Cell(row_num, col_idx, value))
        
        video_links = data_dict.get('video_links', [''] * 5)
        for i, col_idx in enumerate(COL_VIDEO_LINKS):
            if video_links[i]: cell_list.append(gspread.Cell(row_num, col_idx, video_links[i]))
        
        for col, value in manual_values.items():
            if value: cell_list.append(gspread.Cell(row_num, col, value))
        return cell_list
    except: return []

def main():
    print("=" * 60)
    print("📂 YouTube 채널 분석기 v5 - ID 추출 강화 버전")
    print("=" * 60)
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(DATA_TAB_NAME)
        print("✅ 시트 연결 완료")
        
        range_input = os.environ.get('RANGE', '').strip()
        all_data = worksheet.get_all_values()
        if range_input:
            if '-' in range_input: start_row, end_row = map(int, range_input.split('-'))
            else: start_row = end_row = int(range_input)
        else:
            start_row = 2
            end_row = len(all_data)
        
        print(f"📌 {start_row}행 ~ {end_row}행 처리 시작")
        batch_cells = []
        batch_rows_count = 0
        
        for row_num in range(start_row, end_row + 1):
            print(f"\n🔍 [{row_num - start_row + 1}/{end_row - start_row + 1}] Row {row_num}")
            try:
                row_idx = row_num - 1
                if row_idx >= len(all_data): break
                row_data = all_data[row_idx]
                url = row_data[COL_URL - 1] if len(row_data) >= COL_URL else ''
                
                if not url: continue
                
                manual = preserve_manual_columns_batch(all_data, row_num)
                data = get_channel_data_v5(url, row_num, row_data)
                
                if data:
                    cells = build_cell_list(row_num, data, manual, row_data)
                    batch_cells.extend(cells)
                    batch_rows_count += 1
                
                if batch_rows_count >= BATCH_SIZE or row_num == end_row:
                    if batch_cells:
                        worksheet.update_cells(batch_cells)
                        print(f"  📤 저장 완료 ({len(batch_cells)}셀)")
                        batch_cells = []
                        batch_rows_count = 0
                        time.sleep(2)
                
                time.sleep(random.uniform(1, 2))
                
            except Exception as e:
                print(f"  ❌ 에러: {e}")
                traceback.print_exc()
        print("\n✅ 작업 끝")
    except Exception as e:
        print(f"❌ 치명적 오류: {e}")

if __name__ == '__main__':
    main()
