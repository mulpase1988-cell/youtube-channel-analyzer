# ========================================
# YouTube 채널 분석기 - GitHub Actions 버전
# RSS만 사용 (조회수 미수집)
# 채널 ID 자동 저장
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
import sys

# ========================================
# 1. 설정
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
COL_HANDLE = 3
COL_LATEST_UPLOAD = 12     # L: 최근업로드
COL_COLLECT_DATE = 13      # M: 수집일
COL_CHANNEL_ID = 24        # X: channel_id
COL_VIDEO_LINKS = [29, 30, 31, 32, 33]  # AC~AG: 영상 썸네일 1~5

# ========================================
# 2. 범위 입력
# ========================================

def get_range_from_input():
    """처리 범위 가져오기"""
    if len(sys.argv) >= 3:
        try:
            return int(sys.argv[1]), int(sys.argv[2])
        except:
            pass
    
    range_input = os.environ.get('RANGE', '').strip()
    if range_input and '-' in range_input:
        try:
            s, e = map(int, range_input.split('-'))
            return s, e
        except:
            pass
    
    return 2, None

# ========================================
# 3. 채널 ID 추출
# ========================================

def extract_channel_id(handle_or_url):
    """채널 ID 추출"""
    if not handle_or_url:
        return None
    
    url = str(handle_or_url).strip()
    
    # 방법 1: /channel/UC... 직접 추출
    if '/channel/UC' in url:
        try:
            channel_id = url.split('/channel/')[-1].split('/')[0].split('?')[0]
            if channel_id.startswith('UC') and len(channel_id) == 24:
                print(f"  ✅ 방법1 성공: {channel_id}")
                return channel_id
        except:
            pass
    
    # 방법 2: 웹 스크래핑으로 추출
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cookie': 'CONSENT=YES+KR.ko+20150622-23-0'
        }
        
        clean_url = url.split('/shorts')[0].split('/videos')[0]
        if not clean_url.startswith('http'):
            clean_url = f"https://www.youtube.com/{clean_url}"
        
        response = requests.get(clean_url, headers=headers, timeout=10)
        match = re.search(r'"channelId":"(UC[^"]+)"', response.text)
        if match:
            channel_id = match.group(1)
            if channel_id.startswith('UC') and len(channel_id) == 24:
                print(f"  ✅ 방법2 성공: {channel_id}")
                return channel_id
    except Exception as e:
        print(f"  ⚠️ 웹 스크래핑 실패: {e}")
    
    print(f"  ❌ 채널 ID 추출 실패")
    return None

# ========================================
# 4. RSS 데이터 수집
# ========================================

def get_rss_data(channel_id):
    """RSS에서 데이터 수집 (최근 5개 영상)"""
    if not channel_id:
        return None
    
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    print(f"  📡 RSS 데이터 요청 중...")
    
    videos = []
    
    try:
        feed = feedparser.parse(rss_url)
        
        if not feed.entries:
            print(f"  ⚠️ RSS 피드가 비어있음")
            return None
        
        for entry in feed.entries[:5]:  # 최근 5개만
            try:
                # Video ID 추출
                vid = None
                if hasattr(entry, 'yt_videoid'):
                    vid = entry.yt_videoid
                elif 'id' in entry:
                    vid = entry.id.split(':')[-1]
                
                if not vid:
                    continue
                
                # 제목
                title = entry.title if hasattr(entry, 'title') else ''
                
                # 발행 날짜
                pub_date = 'Unknown'
                try:
                    dt = dateutil_parser.parse(entry.published)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    pub_date = dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
                
                # 썸네일 URL
                thumbnail = f"https://i.ytimg.com/vi/{vid}/mqdefault.jpg"
                if hasattr(entry, 'media_thumbnail') and entry.media_thumbnail:
                    thumbnail = entry.media_thumbnail[0]['url']
                
                videos.append({
                    'video_id': vid,
                    'title': title,
                    'published_date': pub_date,
                    'thumbnail_url': thumbnail
                })
                
            except Exception as e:
                print(f"  ⚠️ 항목 파싱 실패: {e}")
                continue
        
        print(f"  ✅ RSS 완료: {len(videos)}개 영상")
        return videos if videos else None
        
    except Exception as e:
        print(f"  ❌ RSS 파싱 실패: {e}")
        return None

# ========================================
# 5. 시트 연결 및 업데이트
# ========================================

def connect_to_sheet():
    """Google Sheets 연결"""
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

def create_cell(row, col, value):
    """셀 객체 생성"""
    return gspread.Cell(row, col, value)

def update_row_data(row_num, channel_id, videos):
    """행 데이터 업데이트"""
    cells = []
    
    # X열: channel_id (자동 저장)
    if channel_id:
        cells.append(create_cell(row_num, COL_CHANNEL_ID, channel_id))
    
    # L열: 최근업로드 (최근 영상 발행일)
    if videos:
        latest_video = videos[0]
        if latest_video['published_date'] != 'Unknown':
            latest_date = latest_video['published_date'].split(' ')[0]
            cells.append(create_cell(row_num, COL_LATEST_UPLOAD, latest_date))
    
    # M열: 수집일 (오늘 날짜)
    collect_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    cells.append(create_cell(row_num, COL_COLLECT_DATE, collect_date))
    
    # AC~AG열: 영상 1~5 썸네일 URL
    for i, col_idx in enumerate(COL_VIDEO_LINKS):
        if i < len(videos):
            thumbnail = videos[i].get('thumbnail_url', '')
            if thumbnail:
                cells.append(create_cell(row_num, col_idx, thumbnail))
    
    return cells

# ========================================
# 6. 메인 실행
# ========================================

def main():
    """메인 실행"""
    print("=" * 70)
    print("🎬 YouTube 채널 분석기 - GitHub Actions 버전")
    print("📡 RSS 데이터만 수집 (최근업로드, 썸네일)")
    print("🔗 채널 ID 자동 저장")
    print("=" * 70)
    print()
    
    try:
        # 시트 연결
        worksheet = connect_to_sheet()
        
        # 데이터 로드
        print("📥 시트 데이터 로드 중...")
        all_data = worksheet.get_all_values()
        print(f"✅ {len(all_data)}행 로드 완료\n")
        
        # 범위 설정
        start_row, end_row = get_range_from_input()
        if end_row is None:
            end_row = len(all_data)
        
        print(f"📌 처리 범위: {start_row}행 ~ {end_row}행")
        print(f"📦 총 {end_row - start_row + 1}개 행\n")
        
        print("=" * 70)
        print("🚀 처리 시작")
        print("=" * 70)
        print()
        
        success_count = 0
        fail_count = 0
        batch_cells = []
        
        for row_num in range(start_row, min(end_row + 1, len(all_data))):
            print(f"[{row_num - start_row + 1}/{end_row - start_row + 1}] Row {row_num} 처리 중...")
            
            try:
                row_data = all_data[row_num - 1]
                
                # Channel ID 가져오기
                channel_id = None
                if len(row_data) >= COL_CHANNEL_ID:
                    existing_id = str(row_data[COL_CHANNEL_ID - 1]).strip()
                    if existing_id.startswith('UC'):
                        channel_id = existing_id
                        print(f"  ✓ 기존 channel_id: {channel_id}")
                
                # Channel ID 없으면 핸들에서 추출
                if not channel_id:
                    if len(row_data) >= COL_HANDLE:
                        handle = str(row_data[COL_HANDLE - 1]).strip()
                        if handle:
                            print(f"  📍 핸들에서 channel_id 추출 시도...")
                            channel_id = extract_channel_id(handle)
                
                if not channel_id:
                    print(f"  ❌ channel_id 없음, 넘어감")
                    fail_count += 1
                    continue
                
                print(f"  🔗 ID: {channel_id}")
                
                # RSS 데이터 수집
                videos = get_rss_data(channel_id)
                
                if not videos:
                    print(f"  ❌ RSS 데이터 없음")
                    fail_count += 1
                    continue
                
                # 행 업데이트
                cells = update_row_data(row_num, channel_id, videos)
                batch_cells.extend(cells)
                
                print(f"  ✅ 완료: 영상 {len(videos)}개, 최근업로드 {videos[0]['published_date'].split(' ')[0]}")
                success_count += 1
                
                # 배치 업데이트 (20개씩)
                if len(batch_cells) >= 20 or row_num == end_row:
                    print(f"  📤 배치 저장: {len(batch_cells)}개 셀")
                    worksheet.update_cells(batch_cells)
                    batch_cells = []
                    print(f"  💤 2초 대기...")
                    time.sleep(2)
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"  ❌ 오류: {e}")
                import traceback
                traceback.print_exc()
                fail_count += 1
                time.sleep(2)
                continue
        
        # 남은 배치 저장
        if batch_cells:
            print(f"\n📤 최종 저장: {len(batch_cells)}개 셀")
            worksheet.update_cells(batch_cells)
        
        print("\n" + "=" * 70)
        print("📊 최종 결과")
        print("=" * 70)
        print(f"✅ 성공: {success_count}개")
        print(f"❌ 실패: {fail_count}개")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ 치명적 오류: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()
