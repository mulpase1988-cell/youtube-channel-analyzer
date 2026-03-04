# ========================================
# YouTube 채널 분석기 - GitHub Actions 버전 (최적화)
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
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
import urllib.parse
import time
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ========================================
# 0. 로깅 설정
# ========================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

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

# 타임아웃 설정
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '10'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '2'))
BATCH_UPDATE_SIZE = int(os.environ.get('BATCH_UPDATE_SIZE', '50'))
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '3'))

# 컬럼 매핑
COL_HANDLE = 3
COL_LATEST_UPLOAD = 12     # L: 최근업로드
COL_COLLECT_DATE = 13      # M: 수집일
COL_CHANNEL_ID = 24        # X: channel_id
COL_VIDEO_LINKS = [29, 30, 31, 32, 33]  # AC~AG: 영상 썸네일 1~5

# ========================================
# 2. Requests 세션 (재시도 로직 포함)
# ========================================

def create_session_with_retry(timeout=REQUEST_TIMEOUT, max_retries=MAX_RETRIES):
    """재시도 로직이 있는 requests 세션"""
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        read=max_retries,
        connect=max_retries,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 503, 504, 408, 429),
        allowed_methods=["GET", "HEAD"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.timeout = timeout
    return session

# 전역 세션
REQUEST_SESSION = create_session_with_retry()

# ========================================
# 3. 범위 입력
# ========================================

def get_range_from_input():
    """처리 범위 가져오기"""
    start_row = int(os.environ.get('START_ROW', '2'))
    end_row_str = os.environ.get('END_ROW', '')
    end_row = int(end_row_str) if end_row_str else None
    
    return start_row, end_row

# ========================================
# 4. 채널 ID 추출 (최적화)
# ========================================

def extract_channel_id_fast(handle_or_url):
    """빠른 채널 ID 추출 (URL 분석 우선)"""
    if not handle_or_url:
        return None
    
    url = str(handle_or_url).strip()
    
    # 방법 1: URL에서 직접 추출 (가장 빠름)
    patterns = [
        r'/channel/(UC[a-zA-Z0-9_-]{22})',
        r'(?:youtube\.com/)?(@[a-zA-Z0-9_.-]+)',
        r'(?:youtube\.com/)([a-zA-Z0-9_-]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            extracted = match.group(1)
            if extracted.startswith('UC') and len(extracted) >= 24:
                logger.info(f"  ✅ URL 패턴 매칭 성공: {extracted}")
                return extracted
    
    # 방법 2: 웹 스크래핑 (타임아웃 설정)
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept-Language': 'ko-KR,ko;q=0.9',
            'Cookie': 'CONSENT=YES+KR.ko'
        }
        
        clean_url = url.split('/shorts')[0].split('/videos')[0]
        if not clean_url.startswith('http'):
            clean_url = f"https://www.youtube.com/{clean_url}"
        
        response = REQUEST_SESSION.get(
            clean_url,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        
        # channelId 검색
        match = re.search(r'"channelId":"(UC[a-zA-Z0-9_-]{22})"', response.text)
        if match:
            channel_id = match.group(1)
            logger.info(f"  ✅ 웹 스크래핑 성공: {channel_id}")
            return channel_id
            
    except requests.Timeout:
        logger.warning(f"  ⚠️ 웹 요청 타임아웃")
    except Exception as e:
        logger.warning(f"  ⚠️ 웹 스크래핑 실패: {str(e)[:50]}")
    
    logger.error(f"  ❌ 채널 ID 추출 실패")
    return None

# ========================================
# 5. RSS 데이터 수집 (최적화)
# ========================================

def get_rss_data(channel_id, max_videos=5):
    """RSS에서 데이터 수집 (최근 N개 영상)"""
    if not channel_id:
        return None
    
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    
    try:
        logger.info(f"  📡 RSS 요청 중...")
        feed = feedparser.parse(rss_url)
        
        if not feed.entries:
            logger.warning(f"  ⚠️ RSS 피드 비어있음")
            return None
        
        videos = []
        
        for entry in feed.entries[:max_videos]:
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
                title = entry.get('title', '')
                
                # 발행 날짜
                pub_date = 'Unknown'
                try:
                    dt = dateutil_parser.parse(entry.published)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    pub_date = dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
                
                # 썸네일 URL (기본값)
                thumbnail = f"https://i.ytimg.com/vi/{vid}/mqdefault.jpg"
                
                videos.append({
                    'video_id': vid,
                    'title': title,
                    'published_date': pub_date,
                    'thumbnail_url': thumbnail
                })
                
            except Exception as e:
                logger.debug(f"  ⚠️ 항목 파싱 실패: {e}")
                continue
        
        if videos:
            logger.info(f"  ✅ RSS 완료: {len(videos)}개 영상")
            return videos
        else:
            logger.warning(f"  ⚠️ RSS 항목 없음")
            return None
        
    except Exception as e:
        logger.error(f"  ❌ RSS 파싱 실패: {str(e)[:50]}")
        return None

# ========================================
# 6. 시트 연결
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
    
    logger.info(f"✅ '{SHEET_NAME}' 시트 연결 성공\n")
    
    return worksheet

def create_cell(row, col, value):
    """셀 객체 생성"""
    return gspread.Cell(row, col, value)

def update_row_data(row_num, channel_id, videos):
    """행 데이터 업데이트 (셀 목록 반환)"""
    cells = []
    
    # X열: channel_id
    if channel_id:
        cells.append(create_cell(row_num, COL_CHANNEL_ID, channel_id))
    
    # L열: 최근업로드
    if videos:
        latest_date = videos[0]['published_date'].split(' ')[0]
        cells.append(create_cell(row_num, COL_LATEST_UPLOAD, latest_date))
    
    # M열: 수집일
    collect_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    cells.append(create_cell(row_num, COL_COLLECT_DATE, collect_date))
    
    # AC~AG열: 썸네일 URL
    for i, col_idx in enumerate(COL_VIDEO_LINKS):
        if i < len(videos):
            thumbnail = videos[i].get('thumbnail_url', '')
            if thumbnail:
                cells.append(create_cell(row_num, col_idx, thumbnail))
    
    return cells

# ========================================
# 7. 행 처리 (병렬화)
# ========================================

def process_row(row_num, row_data, all_data_len):
    """단일 행 처리"""
    try:
        # 기존 Channel ID 확인
        channel_id = None
        if len(row_data) >= COL_CHANNEL_ID:
            existing_id = str(row_data[COL_CHANNEL_ID - 1]).strip()
            if existing_id.startswith('UC'):
                channel_id = existing_id
                logger.info(f"  ✓ 기존 channel_id: {channel_id}")
        
        # Channel ID 없으면 추출
        if not channel_id:
            if len(row_data) >= COL_HANDLE:
                handle = str(row_data[COL_HANDLE - 1]).strip()
                if handle:
                    logger.info(f"  📍 핸들에서 channel_id 추출...")
                    channel_id = extract_channel_id_fast(handle)
        
        if not channel_id:
            logger.warning(f"  ❌ channel_id 없음, 넘어감")
            return {'row': row_num, 'success': False, 'cells': []}
        
        logger.info(f"  🔗 ID: {channel_id}")
        
        # RSS 데이터 수집
        videos = get_rss_data(channel_id)
        
        if not videos:
            logger.warning(f"  ⚠️ RSS 데이터 없음")
            return {'row': row_num, 'success': False, 'cells': []}
        
        # 셀 업데이트 정보
        cells = update_row_data(row_num, channel_id, videos)
        logger.info(f"  ✅ 완료: {len(videos)}개 영상, {videos[0]['published_date'].split(' ')[0]}")
        
        return {'row': row_num, 'success': True, 'cells': cells}
        
    except Exception as e:
        logger.error(f"  ❌ 오류: {str(e)[:50]}")
        return {'row': row_num, 'success': False, 'cells': []}

# ========================================
# 8. 메인 실행
# ========================================

def main():
    """메인 실행"""
    logger.info("=" * 70)
    logger.info("🎬 YouTube 채널 분석기 - 최적화 버전")
    logger.info("📡 RSS 기반 데이터 수집 (병렬 처리)")
    logger.info("=" * 70 + "\n")
    
    try:
        # 시트 연결
        worksheet = connect_to_sheet()
        
        # 데이터 로드
        logger.info("📥 시트 데이터 로드 중...")
        all_data = worksheet.get_all_values()
        logger.info(f"✅ {len(all_data)}행 로드 완료\n")
        
        # 범위 설정
        start_row, end_row = get_range_from_input()
        if end_row is None:
            end_row = len(all_data)
        
        process_count = min(end_row + 1, len(all_data)) - start_row
        logger.info(f"📌 처리 범위: {start_row}행 ~ {min(end_row, len(all_data))}행")
        logger.info(f"📦 총 {process_count}개 행\n")
        
        logger.info("=" * 70)
        logger.info("🚀 처리 시작 (병렬 모드)")
        logger.info("=" * 70 + "\n")
        
        success_count = 0
        fail_count = 0
        batch_cells = []
        processed = 0
        
        # 병렬 처리 (ThreadPoolExecutor 사용)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {}
            
            for row_num in range(start_row, min(end_row + 1, len(all_data))):
                row_data = all_data[row_num - 1]
                future = executor.submit(process_row, row_num, row_data, len(all_data))
                futures[future] = row_num
            
            # 완료된 작업부터 처리
            for future in as_completed(futures):
                try:
                    result = future.result()
                    row_num = result['row']
                    processed += 1
                    
                    logger.info(f"[{processed}/{process_count}] Row {row_num} 완료")
                    
                    if result['success']:
                        success_count += 1
                        batch_cells.extend(result['cells'])
                    else:
                        fail_count += 1
                    
                    # 배치 업데이트 (크기 도달 시)
                    if len(batch_cells) >= BATCH_UPDATE_SIZE:
                        logger.info(f"  📤 배치 저장: {len(batch_cells)}개 셀")
                        worksheet.update_cells(batch_cells)
                        batch_cells = []
                        time.sleep(1)
                        
                except Exception as e:
                    logger.error(f"❌ 작업 실패: {e}")
                    fail_count += 1
        
        # 남은 배치 저장
        if batch_cells:
            logger.info(f"\n📤 최종 저장: {len(batch_cells)}개 셀")
            worksheet.update_cells(batch_cells)
        
        logger.info("\n" + "=" * 70)
        logger.info("📊 최종 결과")
        logger.info("=" * 70)
        logger.info(f"✅ 성공: {success_count}개")
        logger.info(f"❌ 실패: {fail_count}개")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"\n❌ 치명적 오류: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
