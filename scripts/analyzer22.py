# ========================================
# YouTube 채널 분석기 v4 - 하이브리드 스크래핑 (Anti-Block)
# yt-dlp 대체 버전 (Requests + Scrapetube + RSS)
# GitHub Actions IP 차단 우회에 최적화
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

# 컬럼 매핑 (기존 유지)
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
# 2. 스크래핑 헬퍼 함수
# ========================================

def get_channel_id_and_rss(channel_url):
    """채널 URL에서 채널 ID 추출 및 RSS URL 생성"""
    # 1. RSS 피드를 통해 ID 추출 시도 (가장 안전)
    try:
        if 'channel_id' in channel_url: # 이미 ID가 URL에 있는 경우
            cid = channel_url.split('channel_id=')[-1]
            return cid
        
        # Requests로 HTML 가져오기 (헤더 필수)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        res = requests.get(channel_url, headers=headers, timeout=10)
        
        # 메타 태그에서 channelId 찾기
        match = re.search(r'"channelId":"(UC[^"]+)"', res.text)
        if match:
            return match.group(1)
            
        # URL 패턴에서 찾기
        if '/channel/' in channel_url:
            return channel_url.split('/channel/')[-1].split('/')[0]
            
    except Exception as e:
        print(f"  ⚠️ 채널 ID 추출 실패: {e}")
    
    return None

def get_channel_stats_html(channel_url):
    """HTML 파싱으로 구독자, 총조회수, 국가, 썸네일 가져오기"""
    stats = {
        'subscribers': 0,
        'total_views': 0,
        'channel_name': '',
        'channel_thumbnail': '',
        'handle': '',
        'country': '' # HTML에서 국가 추출은 어려움, 기본값 유지
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'ko-KR,ko;q=0.9',
    }
    
    try:
        # 1. 채널 홈/About 페이지 요청
        res = requests.get(channel_url, headers=headers, timeout=10)
        text = res.text
        
        # 2. 채널명
        title_match = re.search(r'<meta property="og:title" content="([^"]+)">', text)
        if title_match:
            stats['channel_name'] = title_match.group(1)
            
        # 3. 채널 썸네일
        thumb_match = re.search(r'<meta property="og:image" content="([^"]+)">', text)
        if thumb_match:
            stats['channel_thumbnail'] = thumb_match.group(1)
            
        # 4. 핸들 (URL 파싱)
        if '@' in channel_url:
            stats['handle'] = '@' + channel_url.split('@')[1].split('/')[0]
        
        # 5. 구독자 수 파싱 (텍스트 패턴 검색)
        # 패턴: "구독자 12.3만명" 또는 "123K subscribers"
        sub_match = re.search(r'"subscriberCountText":\{"accessibility":\{"accessibilityData":\{"label":"([^"]+)"', text)
        if sub_match:
            sub_text = sub_match.group(1) # 예: "구독자 12.3만명"
            stats['subscribers'] = parse_korean_count(sub_text)
            
        # 6. 총 조회수 (About 탭이 아니라 정확하지 않을 수 있으나 메타데이터 시도)
        # 보통 About 탭을 별도 요청해야 하나, 스크래핑 방지로 인해 생략하거나 RSS 데이터로 대체
        
    except Exception as e:
        print(f"  ⚠️ HTML 파싱 에러: {e}")
        
    return stats

def parse_korean_count(text):
    """'구독자 12.3만명', '조회수 1.5천회' 등을 숫자로 변환"""
    try:
        # 숫자와 단위만 남기기
        text = re.sub(r'[^0-9.천만억KMB]', '', text)
        
        multiplier = 1
        if '천' in text or 'K' in text:
            multiplier = 1000
            text = text.replace('천', '').replace('K', '')
        elif '만' in text:
            multiplier = 10000
            text = text.replace('만', '')
        elif '억' in text or 'M' in text:
            multiplier = 100000000
            text = text.replace('억', '').replace('M', '')
        elif 'B' in text:
            multiplier = 1000000000
            text = text.replace('B', '')
            
        return int(float(text) * multiplier)
    except:
        return 0

def get_exact_date_from_rss(channel_id):
    """RSS 피드를 통해 정확한 최신 업로드 날짜 및 영상 정보 가져오기 (최대 15개)"""
    if not channel_id:
        return []
    
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    try:
        feed = feedparser.parse(rss_url)
        videos = []
        for entry in feed.entries:
            published = None
            if hasattr(entry, 'published_parsed'):
                published = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            
            videos.append({
                'video_id': entry.yt_videoid,
                'title': entry.title,
                'published_at': published,
                'views': int(entry.media_statistics['views']) if hasattr(entry, 'media_statistics') else 0
            })
        return videos
    except Exception as e:
        print(f"  ⚠️ RSS 파싱 실패: {e}")
        return []

# ========================================
# 3. 메인 데이터 수집 (하이브리드)
# ========================================

def get_channel_data_hybrid(channel_url, row_num, row_data):
    """Requests + Scrapetube + RSS 조합"""
    
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
    
    print(f"  📡 데이터 수집 중... (Anti-Block Mode)")

    # 1. 채널 ID 추출
    channel_id = get_channel_id_and_rss(clean_url)
    if channel_id:
        result['channel_id'] = channel_id
        print(f"    ✓ 채널 ID: {channel_id}")
    else:
        print("    ❌ 채널 ID 추출 실패")
        return None

    # 2. RSS 피드로 정확한 날짜 및 최신 15개 영상 정보 확보 (매우 빠르고 차단 없음)
    rss_videos = get_exact_date_from_rss(channel_id)
    rss_map = {v['video_id']: v for v in rss_videos}
    
    if rss_videos:
        result['latest_upload'] = rss_videos[0]['published_at'].strftime('%Y-%m-%d')
        result['channel_name'] = rss_videos[0].get('author', '') # RSS에 있는 경우 사용
    
    # 3. HTML 파싱으로 구독자, 썸네일 가져오기
    html_stats = get_channel_stats_html(clean_url)
    if html_stats['subscribers'] > 0:
        result['subscribers'] = html_stats['subscribers']
    if html_stats['channel_thumbnail']:
        result['channel_thumbnail'] = html_stats['channel_thumbnail']
    if html_stats['channel_name']:
        result['channel_name'] = html_stats['channel_name'] # HTML 파싱이 더 정확할 수 있음
    if html_stats['handle']:
        result['handle'] = html_stats['handle']

    # 4. Scrapetube로 최근 영상 30개 상세 조회수 가져오기 (HTML 파싱 방식이라 차단 우회)
    #    scrapetube는 내부 API를 사용하므로 yt-dlp보다 훨씬 가벼움
    print("    ✓ 영상 목록 및 조회수 수집 중...")
    
    combined_videos = []
    try:
        # limit=30: 최근 30개만 가져옴
        videos = scrapetube.get_channel(channel_id, limit=30)
        
        for v in videos:
            video_id = v['videoId']
            
            # 조회수 파싱
            view_count = 0
            if 'viewCountText' in v:
                # "simpleText": "조회수 1.5만회" 또는 "15,030 views" 형태
                view_text = v['viewCountText'].get('simpleText', '') or v['viewCountText'].get('accessibility', {}).get('accessibilityData', {}).get('label', '')
                view_count = parse_korean_count(view_text)
            
            # 썸네일 (가장 큰 것)
            thumb_url = ''
            if 'thumbnail' in v and 'thumbnails' in v['thumbnail']:
                thumb_url = v['thumbnail']['thumbnails'][-1]['url']
                
            # 날짜 (RSS에 있으면 RSS 사용, 없으면 추정 불가 - Scrapetube 날짜는 '2일 전' 텍스트임)
            published_at = None
            if video_id in rss_map:
                published_at = rss_map[video_id]['published_at']
            
            combined_videos.append({
                'id': video_id,
                'views': view_count,
                'thumbnail': thumb_url,
                'published_at': published_at
            })
            
    except Exception as e:
        print(f"    ⚠️ 영상 목록 수집 실패: {e}")

    # 5. 데이터 집계
    # 썸네일 채우기
    for i in range(min(5, len(combined_videos))):
        result['video_links'][i] = combined_videos[i]['thumbnail']
        
    # 조회수 합계
    views_list = [v['views'] for v in combined_videos]
    result['views_5'] = sum(views_list[:5])
    result['views_10'] = sum(views_list[:10])
    result['views_20'] = sum(views_list[:20])
    result['views_30'] = sum(views_list[:30])
    
    # 기간별 조회수 (RSS에 날짜가 있는 15개까지만 정확히 계산 가능)
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
    # 기존 시트의 K열(최초 업로드) 값 유지 로직
    # 만약 기존 값이 없으면, 수집된 것 중 가장 오래된 날짜 사용
    
    existing_first = str(row_data[COL_FIRST_UPLOAD-1]).strip() if len(row_data) >= COL_FIRST_UPLOAD else ''
    
    first_date_dt = None
    if existing_first:
        try:
            first_date_dt = datetime.strptime(existing_first, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        except:
            pass
            
    # 수집된 데이터 중 가장 오래된 날짜와 비교
    if dates:
        min_collected = min(dates)
        if first_date_dt is None or min_collected < first_date_dt:
            first_date_dt = min_collected
    
    if first_date_dt:
        result['first_upload'] = first_date_dt.strftime('%Y-%m-%d')
        if result['latest_upload']:
            last_date_dt = datetime.strptime(result['latest_upload'], '%Y-%m-%d').replace(tzinfo=timezone.utc)
            result['operation_days'] = (last_date_dt - first_date_dt).days

    print(f"    ✅ 완료: 구독자 {result['subscribers']:,}명 | 최근 {result['latest_upload']}")
    return result

# ========================================
# 4. 시트 입출력 (기존 동일)
# ========================================

def preserve_manual_columns_batch(all_sheet_data, row_num):
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
        print(f"❌ 셀 생성 오류: {e}")
        return []

# ========================================
# 5. 메인 실행
# ========================================
def main():
    print("=" * 60)
    print("📂 YouTube 채널 분석기 v4 - 하이브리드 스크래핑 (No API, Anti-Block)")
    print("=" * 60)

    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(DATA_TAB_NAME)
        print("✅ 구글 시트 연결 완료")

        range_input = os.environ.get('RANGE', '').strip()
        all_data = worksheet.get_all_values()
        
        if range_input:
            if '-' in range_input:
                start_row, end_row = map(int, range_input.split('-'))
            else:
                start_row = end_row = int(range_input)
        else:
            start_row = 2
            end_row = len(all_data)

        print(f"📌 대상: {start_row}행 ~ {end_row}행")

        batch_cells = []
        batch_rows_count = 0
        
        for row_num in range(start_row, end_row + 1):
            print(f"\n🔍 [{row_num - start_row + 1}/{end_row - start_row + 1}] Row {row_num} 처리 중...")
            
            try:
                row_idx = row_num - 1
                if row_idx >= len(all_data): break
                row_data = all_data[row_idx]
                url = row_data[COL_URL - 1] if len(row_data) >= COL_URL else ''
                
                if not url:
                    print("  ⏭️ URL 없음")
                    continue
                
                print(f"  📌 URL: {url}")
                
                manual_values = preserve_manual_columns_batch(all_data, row_num)
                data = get_channel_data_hybrid(url, row_num, row_data)
                
                if data:
                    cells = build_cell_list(row_num, data, manual_values, row_data)
                    batch_cells.extend(cells)
                    batch_rows_count += 1
                
                # 배치 업데이트
                if batch_rows_count >= BATCH_SIZE or row_num == end_row:
                    if batch_cells:
                        print(f"  📤 시트 업데이트 중 ({len(batch_cells)}개 셀)...")
                        worksheet.update_cells(batch_cells)
                        print("  ✅ 저장 완료")
                        batch_cells = []
                        batch_rows_count = 0
                        time.sleep(2)

                # 랜덤 딜레이 (차단 방지)
                time.sleep(random.uniform(1.5, 3.0))

            except Exception as e:
                print(f"  ❌ 행 처리 중 오류: {e}")
                traceback.print_exc()

        print("\n✅ 작업 완료")

    except Exception as e:
        print(f"❌ 치명적 오류: {e}")
        traceback.print_exc()

if __name__ == '__main__':
    main()
