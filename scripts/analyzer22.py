# ========================================
# YouTube 채널 분석기 - GitHub Actions 대응 버전
# RSS(날짜) + Scrapetube(조회수) 하이브리드
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
import scrapetube

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
COL_LATEST_UPLOAD = 12    # L: 최근업로드
COL_COLLECT_DATE = 13     # M: 수집일
COL_CHANNEL_ID = 24       # X: channel_id
COL_VIEWS_5D = 25         # Y: 5일조회수합계
COL_VIEWS_10D = 26        # Z: 10일조회수합계
COL_VIEWS_15D = 27        # AA: 15일조회수합계
COL_VIDEO_LINKS = [29, 30, 31, 32, 33]  # AC~AG: 영상1~5

# ========================================
# 2. 유틸리티 함수
# ========================================

def parse_korean_count(text):
    """'조회수 1.2만회', '1.5M views' 등을 정수로 변환"""
    if not text:
        return 0
    
    try:
        # 텍스트 정규화
        text = str(text).strip()
        
        # 숫자와 단위만 남기기
        text = re.sub(r'[^0-9.천만억조KMB]', '', text)
        if not text:
            return 0
        
        # 숫자 부분 추출
        num_match = re.search(r'(\d+\.?\d*)', text)
        if not num_match:
            return 0
        
        num = float(num_match.group(1))
        
        # 단위 적용
        if '조' in text:
            return int(num * 1000000000000)
        elif '억' in text:
            return int(num * 100000000)
        elif '만' in text or 'M' in text:
            return int(num * 10000) if '만' in text else int(num * 1000000)
        elif '천' in text or 'K' in text:
            return int(num * 1000)
        else:
            return int(num)
            
    except Exception as e:
        print(f"  ⚠️ 조회수 파싱 실패: {text} | {e}")
        return 0

def get_range_from_input():
    """처리 범위 설정"""
    # 1. 커맨드라인 인자
    if len(sys.argv) >= 3:
        try:
            start_row = int(sys.argv[1])
            end_row = int(sys.argv[2])
            print(f"✅ 커맨드라인 인자: {start_row}~{end_row}\n")
            return start_row, end_row
        except ValueError:
            pass
    
    # 2. 환경변수
    range_input = os.environ.get('RANGE', '').strip()
    if range_input:
        if '-' in range_input:
            try:
                start_row, end_row = map(int, range_input.split('-'))
                print(f"✅ 환경변수에서 범위: {start_row}~{end_row}\n")
                return start_row, end_row
            except ValueError:
                pass
        else:
            try:
                start_row = int(range_input)
                print(f"✅ 환경변수에서 시작행: {start_row}\n")
                return start_row, None
            except ValueError:
                pass
    
    # 3. 기본값
    print(f"ℹ️ 범위 미지정, 전체 처리\n")
    return 2, None

# ========================================
# 3. 채널 ID 추출
# ========================================

def extract_channel_id(handle_or_url):
    """핸들이나 URL에서 채널 ID 추출"""
    if not handle_or_url:
        return None
    
    handle_or_url = str(handle_or_url).strip()
    print(f"  📍 입력값: {handle_or_url}")
    
    # 방법 1: /channel/UC... 형태 직접 추출
    if '/channel/' in handle_or_url:
        try:
            channel_id = handle_or_url.split('/channel/')[-1].split('/')[0].split('?')[0]
            if channel_id.startswith('UC') and len(channel_id) == 24:
                print(f"  ✅ 방법1 성공 (직접 추출): {channel_id}")
                return channel_id
        except Exception as e:
            print(f"  ⚠️ 방법1 실패: {e}")
    
    # 방법 2: /@핸들명 또는 순수 핸들 → 웹 스크래핑
    try:
        decoded_input = urllib.parse.unquote(handle_or_url)
        
        # @핸들 추출
        handle_match = re.search(r'@([^/\s?]+)', decoded_input)
        if not handle_match:
            handle = decoded_input
        else:
            handle = handle_match.group(1)
        
        print(f"  📌 추출된 핸들: @{handle}")
        
        # 채널 페이지 요청
        page_url = f"https://www.youtube.com/@{handle}"
        print(f"  📡 페이지 로드 중...")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cookie': 'CONSENT=YES+KR.ko+20150622-23-0'
        }
        
        response = requests.get(page_url, headers=headers, timeout=10)
        
        if response.status_code != 200:
            print(f"  ⚠️ 페이지 로드 실패 (상태코드: {response.status_code})")
            return None
        
        # HTML에서 channelId 찾기
        match = re.search(r'"channelId":"(UC[^"]+)"', response.text)
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
# 4. 데이터 수집 핵심 로직 (RSS + Scrapetube)
# ========================================

def get_channel_data_hybrid(channel_id):
    """
    RSS로 정확한 날짜를 얻고, Scrapetube로 정확한 조회수를 얻어 병합
    """
    if not channel_id:
        return None
    
    print(f"  🔗 채널 ID: {channel_id}")
    
    # 1. RSS로 날짜 및 Video ID 가져오기 (가장 정확한 날짜)
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    print(f"  📡 RSS 데이터 요청 중...")
    
    rss_map = {}  # video_id -> published_date
    try:
        feed = feedparser.parse(rss_url)
        
        if not feed.entries:
            print(f"  ⚠️ RSS 피드가 비어있음")
            return []
        
        for entry in feed.entries:
            try:
                # Video ID 추출
                vid = None
                if hasattr(entry, 'yt_videoid'):
                    vid = entry.yt_videoid
                elif 'id' in entry:
                    vid = entry.id.split(':')[-1]
                
                if not vid:
                    continue
                
                # 발행 날짜 추출
                pub_date = 'Unknown'
                try:
                    dt = dateutil_parser.parse(entry.published)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    pub_date = dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
                
                rss_map[vid] = pub_date
                
            except Exception as e:
                print(f"  ⚠️ RSS 항목 파싱 실패: {e}")
                continue
        
        print(f"  ✅ RSS 완료: {len(rss_map)}개 영상")
        
    except Exception as e:
        print(f"  ⚠️ RSS 파싱 실패: {e}")
        return []
    
    # 2. Scrapetube로 영상 목록 및 조회수 가져오기 (차단 우회)
    print(f"  👁️ Scrapetube로 조회수 수집 중...")
    videos = []
    
    try:
        # 최근 20개만 빠르게 가져옴 (sleep_interval 제거)
        scrape_gen = scrapetube.get_channel(channel_id, limit=20)
        
        count = 0
        for v in scrape_gen:
            try:
                video_id = v.get('videoId', '')
                if not video_id:
                    continue
                
                # 제목
                title = ''
                if 'title' in v and 'runs' in v['title']:
                    title = v['title']['runs'][0].get('text', '')
                
                # 조회수 파싱
                view_count = 0
                if 'viewCountText' in v:
                    view_text_obj = v['viewCountText']
                    view_text = ''
                    
                    if 'simpleText' in view_text_obj:
                        view_text = view_text_obj['simpleText']
                    elif 'runs' in view_text_obj and view_text_obj['runs']:
                        view_text = view_text_obj['runs'][0].get('text', '')
                    
                    view_count = parse_korean_count(view_text)
                
                # 썸네일 (여러 크기 중 가장 큰 것)
                thumbnail = f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg"
                if 'thumbnail' in v and 'thumbnails' in v['thumbnail']:
                    thumbnails = v['thumbnail']['thumbnails']
                    if thumbnails:
                        thumbnail = thumbnails[-1].get('url', thumbnail)
                
                # 날짜 (RSS에 있으면 RSS값, 없으면 Unknown)
                pub_date = rss_map.get(video_id, 'Unknown')
                
                videos.append({
                    'video_id': video_id,
                    'title': title,
                    'published_date': pub_date,
                    'views': view_count,
                    'thumbnail_url': thumbnail
                })
                
                count += 1
                
            except Exception as e:
                print(f"  ⚠️ 영상 파싱 실패: {e}")
                continue
        
        print(f"  ✅ Scrapetube 완료: {count}개 영상")
        
    except Exception as e:
        print(f"  ❌ Scrapetube 수집 실패: {e}")
        import traceback
        traceback.print_exc()
        return []
    
    return videos

# ========================================
# 5. 조회수 계산
# ========================================

def calculate_views(videos):
    """최근 일자 기준 5개, 10개, 15개 조회수 합계 계산"""
    
    # 날짜순 정렬 (Unknown은 맨 뒤로)
    sorted_videos = sorted(
        videos,
        key=lambda x: x['published_date'] if x['published_date'] != 'Unknown' else '0000-00-00',
        reverse=True
    )
    
    views_5 = sum(v['views'] for v in sorted_videos[:5])
    views_10 = sum(v['views'] for v in sorted_videos[:10])
    views_15 = sum(v['views'] for v in sorted_videos[:15])
    
    return {
        'views_5': views_5,
        'views_10': views_10,
        'views_15': views_15,
        'videos_5': sorted_videos[:5]
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

def update_row_data(row_num, channel_id, data):
    """행 데이터 업데이트"""
    cells = []
    
    # X열: channel_id
    if channel_id:
        cells.append(create_cell(row_num, COL_CHANNEL_ID, channel_id))
    
    # L열: 최근업로드 (최근 영상 1의 업로드일자, yyyy-mm-dd 형식)
    if data['videos_5']:
        latest_video = data['videos_5'][0]
        if latest_video['published_date'] != 'Unknown':
            latest_date = latest_video['published_date'].split(' ')[0]
            cells.append(create_cell(row_num, COL_LATEST_UPLOAD, latest_date))
    
    # M열: 수집일 (오늘 날짜, yyyy-mm-dd 형식)
    collect_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    cells.append(create_cell(row_num, COL_COLLECT_DATE, collect_date))
    
    # Y열: 5개 영상 조회수 합계
    cells.append(create_cell(row_num, COL_VIEWS_5D, data['views_5']))
    
    # Z열: 10개 영상 조회수 합계
    cells.append(create_cell(row_num, COL_VIEWS_10D, data['views_10']))
    
    # AA열: 15개 영상 조회수 합계
    cells.append(create_cell(row_num, COL_VIEWS_15D, data['views_15']))
    
    # AC~AG열: 영상1~5 썸네일 URL
    for i, col_idx in enumerate(COL_VIDEO_LINKS):
        if i < len(data['videos_5']):
            thumbnail = data['videos_5'][i].get('thumbnail_url', '')
            if thumbnail:
                cells.append(create_cell(row_num, col_idx, thumbnail))
    
    return cells

# ========================================
# 8. 메인 실행
# ========================================

def main():
    """메인 실행 함수"""
    print("=" * 80)
    print("🎬 YouTube 채널 분석기 - Scrapetube 적용 (Anti-Block)")
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
        start_row, end_row = get_range_from_input()
        
        # end_row가 None이면 전체 데이터 처리
        if end_row is None:
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
                
                # 데이터 수집 (Scrapetube + RSS 하이브리드)
                print(f"  🎬 영상 정보 수집 중...")
                videos = get_channel_data_hybrid(channel_id)
                
                if not videos:
                    print(f"  ❌ 영상을 가져올 수 없음, 넘어감")
                    fail_count += 1
                    continue
                
                # 조회수 계산
                stats = calculate_views(videos)
                
                # 시트에 데이터 작성
                cells = update_row_data(row_num, channel_id, stats)
                batch_cells.extend(cells)
                
                print(f"  ✅ Row {row_num} 완료!")
                print(f"     - 최근업로드: {stats['videos_5'][0]['published_date'].split(' ')[0] if stats['videos_5'] else 'N/A'}")
                print(f"     - 수집일: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}")
                print(f"     - 5개 조회수: {stats['views_5']:,}")
                print(f"     - 10개 조회수: {stats['views_10']:,}")
                print(f"     - 15개 조회수: {stats['views_15']:,}")
                
                success_count += 1
                
                # 20개씩 배치 업데이트
                if len(batch_cells) >= 20 or row_num == end_row:
                    print(f"\n📤 배치 업데이트 실행: {len(batch_cells)}개 셀")
                    worksheet.update_cells(batch_cells)
                    print(f"✅ 배치 업데이트 완료!")
                    batch_cells = []
                    print(f"💤 2초 대기...")
                    time.sleep(2)
                
                time.sleep(1)
                
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
