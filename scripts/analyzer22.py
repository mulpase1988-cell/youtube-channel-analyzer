# ========================================
# YouTube 채널 분석기 - GitHub Actions 버전
# Google Sheets + RSS + 웹 스크래핑
# ========================================

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import tempfile
import feedparser
from datetime import datetime, timezone
from dateutil import parser as dateutil_parser
import requests
import re
import urllib.parse
import time

# ========================================
# 1. 환경변수 설정
# ========================================

SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT')
if not SERVICE_ACCOUNT_JSON:
    raise Exception("❌ GOOGLE_SERVICE_ACCOUNT 환경변수가 설정되지 않았습니다")

with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
    f.write(SERVICE_ACCOUNT_JSON)
    SERVICE_ACCOUNT_FILE = f.name

SHEET_NAME = os.environ.get('SHEET_NAME', '유튜브보물창고_테스트')
DATA_TAB_NAME = os.environ.get('DATA_TAB_NAME', '데이터')

# 컬럼 매핑
COL_HANDLE = 3            # C: 핸들
COL_LATEST_UPLOAD = 12    # L: 최근업로드 (최근 영상 1의 업로드일자)
COL_COLLECT_DATE = 13     # M: 수집일 (실행 날짜)
COL_CHANNEL_ID = 24       # X: channel_id
COL_VIEWS_5D = 25         # Y: 5일조회수합계
COL_VIEWS_10D = 26        # Z: 10일조회수합계
COL_VIEWS_15D = 27        # AA: 15일조회수합계
COL_VIDEO_LINKS = [29, 30, 31, 32, 33]  # AC~AG: 영상1~5

# ========================================
# 2. 채널 ID 추출
# ========================================

def extract_channel_id(handle_or_url):
    """
    핸들이나 URL에서 채널 ID 추출
    1. /channel/UC... → 직접 추출
    2. /@핸들명 → 웹 스크래핑으로 추출
    실패하면 None 반환
    """
    
    if not handle_or_url or not str(handle_or_url).strip():
        return None
    
    handle_or_url = str(handle_or_url).strip()
    
    print(f"  📍 입력값: {handle_or_url}")
    
    # ========================================
    # 방법 1: /channel/UC... 형태 직접 추출
    # ========================================
    if '/channel/' in handle_or_url:
        try:
            channel_id = handle_or_url.split('/channel/')[-1].split('/')[0].split('?')[0]
            if channel_id.startswith('UC') and len(channel_id) == 24:
                print(f"  ✅ 방법1 성공 (직접 추출): {channel_id}")
                return channel_id
        except Exception as e:
            print(f"  ⚠️ 방법1 실패: {e}")
    
    # ========================================
    # 방법 2: /@핸들명 형태 또는 순수 핸들 → 웹 스크래핑
    # ========================================
    try:
        # URL 디코딩
        decoded_input = urllib.parse.unquote(handle_or_url)
        
        # @핸들 추출
        handle_match = re.search(r'@([^/\s?]+)', decoded_input)
        if not handle_match:
            # @ 없으면 입력값 자체가 핸들로 간주
            handle = decoded_input
        else:
            handle = handle_match.group(1)
        
        print(f"  📌 추출된 핸들: @{handle}")
        
        # 채널 페이지 요청
        page_url = f"https://www.youtube.com/@{handle}"
        print(f"  📡 페이지 로드 중: {page_url}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(page_url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            print(f"  ⚠️ 페이지 로드 실패 (상태코드: {response.status_code})")
            return None
        
        # HTML에서 channelId 찾기
        match = re.search(r'"channelId":"([^"]+)"', response.text)
        if match:
            channel_id = match.group(1)
            if channel_id.startswith('UC') and len(channel_id) == 24:
                print(f"  ✅ 방법2 성공 (웹 스크래핑): {channel_id}")
                return channel_id
        
        print(f"  ⚠️ 방법2 실패: HTML에서 channelId를 찾을 수 없음")
        return None
        
    except Exception as e:
        print(f"  ⚠️ 방법2 실패: {e}")
        return None

# ========================================
# 3. RSS 피드 파싱
# ========================================

def parse_rss_feed(channel_id, max_videos=15):
    """YouTube RSS 피드에서 최신 영상 정보 추출"""
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    
    print(f"  📡 RSS 피드 다운로드 중...")
    
    try:
        feed = feedparser.parse(rss_url)
        
        if not feed.entries:
            print("  ❌ RSS 피드가 비어있습니다.")
            return []
        
        total_entries = len(feed.entries)
        actual_max = min(max_videos, total_entries)
        
        print(f"  ✅ RSS 피드 로드 완료: {total_entries}개 항목\n")
        
        videos = []
        for i, entry in enumerate(feed.entries[:actual_max]):
            try:
                # Video ID 추출
                video_id = entry.yt_videoid if hasattr(entry, 'yt_videoid') else None
                if not video_id and 'id' in entry:
                    video_id = entry.id.split(':')[-1]
                
                # 제목
                title = entry.title if hasattr(entry, 'title') else ''
                
                # 발행 날짜
                published_str = entry.published if hasattr(entry, 'published') else None
                published_datetime = None
                published_date_str = 'Unknown'
                
                try:
                    published_datetime = dateutil_parser.parse(published_str)
                    if published_datetime.tzinfo is None:
                        published_datetime = published_datetime.replace(tzinfo=timezone.utc)
                    published_date_str = published_datetime.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    published_date_str = 'Unknown'
                
                # 썸네일 URL (고해상도 우선)
                thumbnail_url = ''
                if hasattr(entry, 'media_thumbnail'):
                    thumbnail_url = entry.media_thumbnail[0]['url'] if entry.media_thumbnail else ''
                
                if not thumbnail_url and video_id:
                    thumbnail_url = f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg"
                
                videos.append({
                    'number': i + 1,
                    'video_id': video_id,
                    'title': title,
                    'published_date': published_date_str,
                    'published_datetime': published_datetime,
                    'thumbnail_url': thumbnail_url,
                    'views': 0
                })
                
            except Exception as e:
                print(f"  ⚠️ RSS 항목 파싱 실패: {e}")
                continue
        
        return videos
    
    except Exception as e:
        print(f"  ❌ RSS 오류: {e}")
        return []

# ========================================
# 4. 조회수 추출
# ========================================

def get_video_views(video_id):
    """YouTube 페이지에서 조회수 추출"""
    try:
        url = f"https://www.youtube.com/watch?v={video_id}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return 0
        
        match = re.search(r'"viewCount":"(\d+)"', response.text)
        if match:
            return int(match.group(1))
        
        match = re.search(r'viewCount["\']?\s*:\s*["\']?(\d+)', response.text)
        if match:
            return int(match.group(1))
        
        return 0
        
    except Exception as e:
        print(f"  ⚠️ 조회수 추출 실패 ({video_id}): {e}")
        return 0

# ========================================
# 5. 구간별 조회수 계산
# ========================================

def calculate_views_by_period(videos):
    """최근 일자 기준으로 5개, 10개, 15개 영상의 조회수 합계 계산"""
    
    sorted_videos = sorted(
        videos,
        key=lambda x: x['published_datetime'] if x['published_datetime'] else datetime.min,
        reverse=True
    )
    
    views_5 = sum(v['views'] for v in sorted_videos[:5])
    views_10 = sum(v['views'] for v in sorted_videos[:10])
    views_15 = sum(v['views'] for v in sorted_videos[:15])
    
    return {
        'views_5': views_5,
        'views_10': views_10,
        'views_15': views_15,
        'videos_5': sorted_videos[:5],
        'videos_10': sorted_videos[:10],
        'videos_15': sorted_videos[:15]
    }

# ========================================
# 6. Google Sheets 연결
# ========================================

def connect_to_sheet():
    """Google Sheets에 연결"""
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT_FILE, scope
    )
    gc = gspread.authorize(creds)
    
    spreadsheet = gc.open(SHEET_NAME)
    worksheet = spreadsheet.worksheet(DATA_TAB_NAME)
    
    print(f"✅ '{SHEET_NAME}' 시트 연결 성공\n")
    
    return worksheet

# ========================================
# 7. 배치 업데이트
# ========================================

def create_cell(row, col, value):
    """gspread.Cell 객체 생성"""
    return gspread.Cell(row, col, value)

def update_row_data(worksheet, row_num, channel_id, period_data):
    """행 데이터 업데이트"""
    cells = []
    
    # X열: channel_id
    if channel_id:
        cells.append(create_cell(row_num, COL_CHANNEL_ID, channel_id))
    
    # L열: 최근업로드 (최근 영상 1의 업로드일자, yyyy-mm-dd 형식)
    if period_data['videos_5']:
        latest_video = period_data['videos_5'][0]
        if latest_video['published_date'] != 'Unknown':
            # published_date는 'YYYY-MM-DD HH:MM:SS' 형식이므로 날짜 부분만 추출
            latest_date = latest_video['published_date'].split(' ')[0]
            cells.append(create_cell(row_num, COL_LATEST_UPLOAD, latest_date))
    
    # M열: 수집일 (오늘 날짜, yyyy-mm-dd 형식)
    collect_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    cells.append(create_cell(row_num, COL_COLLECT_DATE, collect_date))
    
    # Y열: 5개 영상 조회수 합계
    cells.append(create_cell(row_num, COL_VIEWS_5D, period_data['views_5']))
    
    # Z열: 10개 영상 조회수 합계
    cells.append(create_cell(row_num, COL_VIEWS_10D, period_data['views_10']))
    
    # AA열: 15개 영상 조회수 합계
    cells.append(create_cell(row_num, COL_VIEWS_15D, period_data['views_15']))
    
    # AC~AG열: 영상1~5 썸네일 URL
    for i, col_idx in enumerate(COL_VIDEO_LINKS):
        if i < len(period_data['videos_5']):
            thumbnail = period_data['videos_5'][i].get('thumbnail_url', '')
            if thumbnail:
                cells.append(create_cell(row_num, col_idx, thumbnail))
    
    return cells

# ========================================
# 8. 메인 실행
# ========================================

def main():
    """메인 실행 함수"""
    print("=" * 80)
    print("🎬 YouTube 채널 분석기 - GitHub Actions 버전")
    print("=" * 80)
    print()
    
    try:
        # 시트 연결
        worksheet = connect_to_sheet()
        
        # 모든 데이터 로드
        print("📥 시트 데이터 로드 중...")
        all_data = worksheet.get_all_values()
        print(f"✅ {len(all_data)}행 데이터 로드 완료\n")
        
        # 처리 범위 설정
        range_input = os.environ.get('RANGE', '').strip()
        if range_input and '-' in range_input:
            start_row, end_row = map(int, range_input.split('-'))
        else:
            start_row = 2
            end_row = len(all_data)
        
        print(f"📌 처리 범위: {start_row}행 ~ {end_row}행")
        print(f"📦 총 {end_row - start_row + 1}개 행 처리 예정\n")
        
        print("=" * 80)
        print("🚀 채널 분석 시작")
        print("=" * 80)
        print()
        
        success_count = 0
        fail_count = 0
        batch_cells = []
        
        for row_num in range(start_row, min(end_row + 1, len(all_data))):
            print(f"\n[{row_num - start_row + 1}/{end_row - start_row + 1}] Row {row_num} 처리 중...")
            print("-" * 80)
            
            try:
                row_data = all_data[row_num - 1]
                
                # channel_id 가져오기 (X열)
                channel_id = None
                if len(row_data) >= COL_CHANNEL_ID:
                    existing_channel_id = str(row_data[COL_CHANNEL_ID - 1]).strip()
                    if existing_channel_id and existing_channel_id.startswith('UC'):
                        channel_id = existing_channel_id
                        print(f"  ✓ 기존 channel_id 사용: {channel_id}")
                
                # channel_id 없으면 핸들(C열)에서 추출
                if not channel_id:
                    if len(row_data) >= COL_HANDLE:
                        handle = str(row_data[COL_HANDLE - 1]).strip()
                        if handle:
                            print(f"  📍 핸들에서 channel_id 추출 시도...")
                            channel_id = extract_channel_id(handle)
                
                if not channel_id:
                    print(f"  ❌ channel_id를 가져올 수 없음, 넘어감")
                    fail_count += 1
                    continue
                
                # RSS 피드 파싱
                print(f"  🎬 영상 정보 수집 중...")
                videos = parse_rss_feed(channel_id, max_videos=15)
                
                if not videos:
                    print(f"  ❌ 영상을 가져올 수 없음, 넘어감")
                    fail_count += 1
                    continue
                
                # 조회수 추출
                print(f"  👁️ 조회수 추출 중...")
                for video in videos:
                    print(f"    - {video['title'][:50]}...")
                    views = get_video_views(video['video_id'])
                    video['views'] = views
                    time.sleep(1)
                
                # 구간별 조회수 계산
                period_data = calculate_views_by_period(videos)
                
                # 시트에 데이터 작성
                cells = update_row_data(worksheet, row_num, channel_id, period_data)
                batch_cells.extend(cells)
                
                print(f"  ✅ Row {row_num} 완료!")
                print(f"     - 최근업로드: {period_data['videos_5'][0]['published_date'].split(' ')[0] if period_data['videos_5'] else 'N/A'}")
                print(f"     - 수집일: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}")
                print(f"     - 5개 조회수: {period_data['views_5']:,}")
                print(f"     - 10개 조회수: {period_data['views_10']:,}")
                print(f"     - 15개 조회수: {period_data['views_15']:,}")
                
                success_count += 1
                
                # 20개씩 배치 업데이트
                if len(batch_cells) >= 20 or row_num == end_row:
                    print(f"\n📤 배치 업데이트 실행: {len(batch_cells)}개 셀")
                    worksheet.update_cells(batch_cells)
                    print(f"✅ 배치 업데이트 완료!")
                    batch_cells = []
                    print(f"💤 2초 대기...")
                    time.sleep(2)
                
                time.sleep(3)
                
            except Exception as e:
                print(f"  ❌ 오류: {e}")
                import traceback
                traceback.print_exc()
                fail_count += 1
                time.sleep(5)
                continue
        
        # 남은 배치 업데이트
        if batch_cells:
            print(f"\n📤 최종 배치 업데이트: {len(batch_cells)}개 셀")
            worksheet.update_cells(batch_cells)
            print(f"✅ 최종 배치 업데이트 완료!")
        
        print("\n" + "=" * 80)
        print("📊 최종 결과")
        print("=" * 80)
        print(f"✅ 성공: {success_count}개")
        print(f"❌ 실패: {fail_count}개")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
