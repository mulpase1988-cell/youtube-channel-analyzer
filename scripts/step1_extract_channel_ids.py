# scripts/step1_extract_channel_ids.py
"""
Step 1: YouTube 채널ID 추출 (URL 디코딩 + Search API 사용)
"""

import os
import json
import logging
import time
import re
import sys
import urllib.parse
from datetime import datetime, timezone

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# 설정
# ============================================================================

SHEET_NAME = '유튜브보물창고_테스트'
DATA_TAB_NAME = '데이터'
API_TAB_NAME = 'API_키_관리'

# 컬럼 (0-based)
COL_CHANNEL_NAME = 0
COL_URL = 1
COL_HANDLE = 2
COL_CHANNEL_ID = 23

# 데이터 파일
DATA_DIR = 'data'
CHANNEL_IDS_FILE = os.path.join(DATA_DIR, 'channel_ids.json')

# ============================================================================
# Google Sheets 연결
# ============================================================================

def init_google_sheets():
    """Google Sheets 및 YouTube API 초기화"""
    
    service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT')
    if not service_account_json:
        logger.error("❌ GOOGLE_SERVICE_ACCOUNT 환경변수가 없습니다")
        raise ValueError("GOOGLE_SERVICE_ACCOUNT not found")
    
    try:
        service_account_info = json.loads(service_account_json)
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON 파싱 실패: {e}")
        raise
    
    # ✅ 올바른 스코프 정의
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    credentials = Credentials.from_service_account_info(
        service_account_info,
        scopes=scopes
    )
    
    gc = gspread.authorize(credentials)
    
    try:
        spreadsheet = gc.open(SHEET_NAME)
        logger.info(f"✅ 스프레드시트 '{SHEET_NAME}' 연결 성공")
    except gspread.SpreadsheetNotFound:
        logger.error(f"❌ 스프레드시트 '{SHEET_NAME}'을 찾을 수 없습니다")
        raise
    
    try:
        worksheet_data = spreadsheet.worksheet(DATA_TAB_NAME)
        logger.info(f"✅ '{DATA_TAB_NAME}' 워크시트 연결 성공")
    except gspread.WorksheetNotFound:
        logger.error(f"❌ 워크시트 '{DATA_TAB_NAME}'을 찾을 수 없습니다")
        raise
    
    try:
        worksheet_api = spreadsheet.worksheet(API_TAB_NAME)
        logger.info(f"✅ '{API_TAB_NAME}' 워크시트 연결 성공")
    except gspread.WorksheetNotFound:
        logger.error(f"❌ 워크시트 '{API_TAB_NAME}'을 찾을 수 없습니다")
        raise
    
    return spreadsheet, worksheet_data, worksheet_api, credentials

# ============================================================================
# API 키 로드
# ============================================================================

def load_api_keys(worksheet_api):
    """API_키_관리 탭에서 활성화된 API 키 로드 (4행부터 시작)"""
    
    try:
        all_values = worksheet_api.get_all_values()
        
        if len(all_values) < 4:
            logger.warning("⚠️ API 키 데이터가 없습니다")
            return []
        
        # 3번째 행(인덱스 2)이 헤더
        headers = all_values[2]
        
        try:
            idx_name = headers.index('키 이름')
            idx_key = headers.index('API 키')
            idx_status = headers.index('활성화')
        except ValueError as e:
            logger.error(f"❌ 필수 컬럼을 찾을 수 없습니다: {e}")
            logger.error(f"   실제 헤더: {headers}")
            return []
        
        api_keys = []
        
        # 4행(인덱스 3)부터 시작
        for row_idx, row in enumerate(all_values[3:], start=4):
            if len(row) <= max(idx_name, idx_key, idx_status):
                continue
            
            key_name = row[idx_name].strip() if len(row) > idx_name else ""
            key_value = row[idx_key].strip() if len(row) > idx_key else ""
            status = row[idx_status].strip() if len(row) > idx_status else ""
            
            # 활성화된 키만
            if key_value and status.upper() in ['TRUE', 'YES', 'O', '활성화', '사용']:
                api_keys.append({
                    'name': key_name,
                    'key': key_value,
                    'row': row_idx
                })
                masked_key = key_value[:10] + '...' + key_value[-5:]
                logger.info(f"   ✓ {key_name}: {masked_key}")
        
        logger.info(f"✅ API 키 {len(api_keys)}개 로드 완료")
        return api_keys
    
    except Exception as e:
        logger.error(f"❌ API 키 로드 실패: {e}")
        return []

# ============================================================================
# 채널ID 추출 함수들
# ============================================================================

def decode_url_handle(handle):
    """URL 인코딩된 핸들 디코딩"""
    try:
        return urllib.parse.unquote(handle)
    except:
        return handle

def extract_channel_id_from_url(url):
    """URL에서 channel_id 추출"""
    
    if not url:
        return None
    
    # URL 디코딩
    url = decode_url_handle(url)
    
    # /channel/UC... 형식
    if '/channel/' in url:
        match = re.search(r'/channel/(UC[a-zA-Z0-9_-]+)', url)
        if match:
            return match.group(1)
    
    # /@handle 형식
    if '/@' in url:
        match = re.search(r'/@([a-zA-Z0-9_\-가-힣]+)', url)
        if match:
            return '@' + match.group(1)
    
    return None

def get_channel_id_from_handle_search(handle, api_key):
    """Search API로 채널 찾기 (forHandle 대체)"""
    
    if not api_key or not handle:
        return None
    
    try:
        handle_clean = handle.lstrip('@')
        logger.info(f"   🔍 Search API로 검색: {handle_clean}")
        
        youtube = build('youtube', 'v3', developerKey=api_key)
        
        response = youtube.search().list(
            part='snippet',
            q=handle_clean,
            type='channel',
            maxResults=5,
            order='relevance'
        ).execute()
        
        if response.get('items'):
            channel_id = response['items'][0]['id']['channelId']
            channel_title = response['items'][0]['snippet']['title']
            logger.info(f"   ✓ Search API: {handle_clean} → {channel_id} ({channel_title})")
            return channel_id
        
        logger.warning(f"   ✗ Search API: {handle_clean} 찾을 수 없음")
        return None
    
    except HttpError as e:
        if e.resp.status == 429:
            logger.warning(f"   ⚠️ Rate Limit 초과 - 60초 대기")
            time.sleep(60)
        else:
            logger.warning(f"   ✗ API 오류: {e.resp.status}")
        return None
    except Exception as e:
        logger.warning(f"   ✗ API 예외: {e}")
        return None

def extract_channel_id(url, handle, api_key):
    """우선순위에 따라 channel_id 추출"""
    
    # URL 디코딩
    url = decode_url_handle(url) if url else ""
    handle = decode_url_handle(handle) if handle else ""
    
    # 1. URL에서 직접 추출
    if url:
        channel_id = extract_channel_id_from_url(url)
        if channel_id and channel_id.startswith('UC'):
            logger.info(f"   ✓ URL 직접 추출: {channel_id}")
            return channel_id
        
        # URL에서 @handle 추출
        if channel_id and channel_id.startswith('@'):
            handle = channel_id
    
    # 핸들이 없으면 실패
    if not handle:
        logger.warning(f"   ✗ URL과 핸들 모두 없음")
        return None
    
    # 2. Search API로 조회
    logger.info(f"   ℹ️ 핸들: {handle}")
    channel_id = get_channel_id_from_handle_search(handle, api_key)
    
    if channel_id:
        return channel_id
    
    logger.error(f"   ✗ 채널ID 추출 실패")
    return None

# ============================================================================
# 범위 파싱 (수정됨)
# ============================================================================

def parse_range(range_str, total_rows):
    """RANGE 환경변수 파싱 (1-10 또는 1-101 형식)"""
    
    if not range_str:
        return (2, total_rows)  # 헤더 제외
    
    try:
        range_str = range_str.strip()
        
        if '-' in range_str:
            parts = range_str.split('-')
            start = int(parts[0].strip())
            end = int(parts[1].strip())
            return (max(start, 2), min(end, total_rows))  # 헤더 제외, 범위 제한
        else:
            # 숫자 하나면 그 행까지
            num = int(range_str)
            return (2, min(num, total_rows))
    except Exception as e:
        logger.warning(f"⚠️ RANGE 파싱 실패: {range_str} ({e})")
        return (2, total_rows)

# ============================================================================
# Step 1 메인 함수
# ============================================================================

def process_step1():
    """Step 1: YouTube 채널ID 추출"""
    
    logger.info("=" * 80)
    logger.info("🚀 Step 1: YouTube 채널ID 추출 시작")
    logger.info("=" * 80)
    
    try:
        # [1/6] Google Sheets 연결
        logger.info("\n[1/6] Google Sheets 연결 중...")
        spreadsheet, worksheet_data, worksheet_api, credentials = init_google_sheets()
        
        # [2/6] 데이터 로드
        logger.info("\n[2/6] 데이터 로드 중...")
        all_values = worksheet_data.get_all_values()
        logger.info(f"✅ 총 {len(all_values)}행 로드 완료")
        
        # [3/6] 처리 범위 결정
        logger.info("\n[3/6] 처리 범위 결정...")
        range_str = os.getenv('RANGE', '').strip()
        total_rows = len(all_values)
        start_row, end_row = parse_range(range_str, total_rows)
        
        logger.info(f"   RANGE 환경변수: '{range_str}'")
        logger.info(f"✅ 범위: {start_row}~{end_row}행 (총 {end_row - start_row + 1}행)")
        
        # [4/6] API 키 로드
        logger.info("\n[4/6] API 키 로드 중...")
        api_keys = load_api_keys(worksheet_api)
        api_key = api_keys[0]['key'] if api_keys else None
        
        if api_key:
            logger.info("✅ API 키 로드 성공")
        else:
            logger.warning("⚠️ API 키 없음 - 웹 스크래핑만 가능")
        
        # [5/6] 채널ID 추출
        logger.info("\n[5/6] 채널ID 추출 중...")
        channel_ids_data = []
        skipped_count = 0
        extracted_count = 0
        failed_count = 0
        
        for row_num in range(start_row, end_row + 1):
            row_idx = row_num - 1  # 0-based
            
            if row_idx >= len(all_values):
                logger.warning(f"Row {row_num}: 범위 초과")
                continue
            
            row_data = all_values[row_idx]
            
            # 각 열 값 추출
            channel_name = row_data[COL_CHANNEL_NAME] if len(row_data) > COL_CHANNEL_NAME else ""
            url = row_data[COL_URL] if len(row_data) > COL_URL else ""
            handle = row_data[COL_HANDLE] if len(row_data) > COL_HANDLE else ""
            existing_channel_id = row_data[COL_CHANNEL_ID] if len(row_data) > COL_CHANNEL_ID else ""
            
            # 기존 channel_id가 있으면 스킵
            if existing_channel_id and existing_channel_id.startswith('UC'):
                logger.info(f"Row {row_num}: ⏭️ 기존 channel_id: {existing_channel_id}")
                skipped_count += 1
                continue
            
            logger.info(f"\n▶ Row {row_num}: {channel_name}")
            logger.info(f"  URL: {url}, 핸들: {handle}")
            
            # 채널ID 추출
            channel_id = extract_channel_id(url, handle, api_key)
            
            if channel_id and channel_id.startswith('UC'):
                logger.info(f"  ✅ 채널ID: {channel_id}")
                channel_ids_data.append({
                    'row': row_num,
                    'channel_name': channel_name,
                    'url': url,
                    'handle': handle,
                    'channel_id': channel_id
                })
                extracted_count += 1
            else:
                logger.error(f"  ❌ 추출 실패")
                failed_count += 1
            
            # API 레이트 리미트 대비 대기
            time.sleep(1)
        
        # [6/6] 결과 저장
        logger.info("\n[6/6] 결과 저장 중...")
        os.makedirs(DATA_DIR, exist_ok=True)
        
        with open(CHANNEL_IDS_FILE, 'w', encoding='utf-8') as f:
            json.dump(channel_ids_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✅ 결과 저장: {CHANNEL_IDS_FILE}")
        
        # 요약
        logger.info("\n" + "=" * 80)
        logger.info("📊 Step 1 완료 요약")
        logger.info("=" * 80)
        logger.info(f"✅ 추출 성공: {extracted_count}개")
        logger.info(f"⏭️ 스킵 (기존 ID): {skipped_count}개")
        logger.info(f"❌ 추출 실패: {failed_count}개")
        logger.info(f"📁 저장 파일: {CHANNEL_IDS_FILE}")
        logger.info("=" * 80)
    
    except Exception as e:
        logger.error(f"\n❌ Step 1 실패: {e}", exc_info=True)
        sys.exit(1)

# ============================================================================
# 실행
# ============================================================================

if __name__ == '__main__':
    process_step1()
