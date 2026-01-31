# scripts/step1_extract_channel_ids.py
"""
Step 1: YouTube 채널ID 추출
- Google Sheets에서 URL/핸들을 읽어 channel_id 추출
- 기존 channel_id가 있으면 스킵
- 없는 경우만 API/웹 스크래핑으로 추출
- 결과를 data/channel_ids.json에 저장
"""

import os
import json
import logging
import time
import re
import sys
from urllib.parse import urlparse
from datetime import datetime

# Google Sheets 및 YouTube API 라이브러리
import gspread
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# config 임포트
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import (
    SHEET_NAME, DATA_TAB_NAME, API_TAB_NAME,
    COL_CHANNEL_NAME, COL_URL, COL_HANDLE, COL_CHANNEL_ID,
    COL_API_KEY_NUMBER, COL_API_KEY_NAME, COL_API_KEY_VALUE, COL_API_KEY_STATUS,
    API_KEY_DATA_START_ROW,
    CHANNEL_IDS_FILE, get_data_dir, get_now_utc, LOG_FORMAT
)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# ============================================================================
# 1. Google Sheets 연결 및 초기화
# ============================================================================

def init_google_sheets():
    """
    Google Sheets 클라이언트 초기화 및 워크북 연결
    환경변수: GOOGLE_SERVICE_ACCOUNT (JSON 형식)
    반환: (spreadsheet, worksheet_data, worksheet_api)
    """
    service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT')
    if not service_account_json:
        logger.error("❌ GOOGLE_SERVICE_ACCOUNT 환경변수가 없습니다.")
        raise ValueError("GOOGLE_SERVICE_ACCOUNT not found")
    
    try:
        # JSON 문자열을 딕셔너리로 변환
        service_account_info = json.loads(service_account_json)
    except json.JSONDecodeError as e:
        logger.error(f"❌ GOOGLE_SERVICE_ACCOUNT JSON 파싱 실패: {e}")
        raise
    
    # gspread 클라이언트 초기화
    credentials = Credentials.from_service_account_info(
        service_account_info,
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    
    gc = gspread.authorize(credentials)
    
    # 워크북 열기
    try:
        spreadsheet = gc.open(SHEET_NAME)
        logger.info(f"✅ 스프레드시트 '{SHEET_NAME}' 연결 성공")
    except gspread.SpreadsheetNotFound:
        logger.error(f"❌ 스프레드시트 '{SHEET_NAME}'을 찾을 수 없습니다.")
        raise
    
    # 데이터 탭
    try:
        worksheet_data = spreadsheet.worksheet(DATA_TAB_NAME)
        logger.info(f"✅ '{DATA_TAB_NAME}' 워크시트 연결 성공")
    except gspread.WorksheetNotFound:
        logger.error(f"❌ 워크시트 '{DATA_TAB_NAME}'을 찾을 수 없습니다.")
        raise
    
    # API 키 탭
    try:
        worksheet_api = spreadsheet.worksheet(API_TAB_NAME)
        logger.info(f"✅ '{API_TAB_NAME}' 워크시트 연결 성공")
    except gspread.WorksheetNotFound:
        logger.error(f"❌ 워크시트 '{API_TAB_NAME}'을 찾을 수 없습니다.")
        raise
    
    return spreadsheet, worksheet_data, worksheet_api

# ============================================================================
# 2. Google Sheets에서 API 키 로드
# ============================================================================

def load_api_keys_from_sheet(worksheet_api):
    """
    Google Sheets의 API_키_관리 탭에서 활성화된 API 키 로드
    열 구조: A(번호), B(키 이름), C(API 키), D(상태)
    데이터 시작: 4행부터
    반환: [{'name': '메인키', 'key': 'AIzaSy...', 'status': '활성화', 'row': 4}, ...]
    """
    try:
        all_values = worksheet_api.get_all_values()
        api_keys = []
        
        for idx, row in enumerate(all_values[API_KEY_DATA_START_ROW - 1:], start=API_KEY_DATA_START_ROW):
            if len(row) > COL_API_KEY_VALUE:
                key_name = row[COL_API_KEY_NAME] if len(row) > COL_API_KEY_NAME else ""
                key_value = row[COL_API_KEY_VALUE] if len(row) > COL_API_KEY_VALUE else ""
                status = row[COL_API_KEY_STATUS] if len(row) > COL_API_KEY_STATUS else ""
                
                # 키값이 있고 상태가 '활성화'인 경우만 포함
                if key_value and status == '활성화':
                    api_keys.append({
                        'name': key_name,
                        'key': key_value,
                        'status': status,
                        'row': idx
                    })
        
        logger.info(f"✅ API 키 {len(api_keys)}개 로드 완료")
        for key in api_keys:
            masked_key = key['key'][:10] + '...' + key['key'][-5:]
            logger.info(f"   - {key['name']}: {masked_key}")
        
        return api_keys
    
    except Exception as e:
        logger.error(f"❌ API 키 로드 실패: {e}")
        return []

def get_first_available_api_key(api_keys):
    """
    첫 번째 활성화된 API 키 반환
    """
    if api_keys:
        return api_keys[0]['key']
    return None

# ============================================================================
# 3. 채널ID 추출 함수들
# ============================================================================

def extract_channel_id_from_url(url):
    """
    URL에서 직접 channel_id 추출
    예시:
    - https://www.youtube.com/channel/UC0lNTQEW6LnTw1V3pn7HvdA → UC0lNTQEW6LnTw1V3pn7HvdA
    - https://www.youtube.com/@skywheel → @skywheel (추후 처리)
    """
    if not url:
        return None
    
    # /channel/UC... 형식
    if '/channel/' in url:
        match = re.search(r'/channel/(UC[a-zA-Z0-9_-]+)', url)
        if match:
            return match.group(1)
    
    # /@handle 형식 (나중에 API로 처리)
    if '/@' in url:
        match = re.search(r'/@([a-zA-Z0-9_-]+)', url)
        if match:
            return '@' + match.group(1)
    
    return None

def is_ascii_only(text):
    """
    텍스트가 ASCII만 포함하는지 확인
    """
    try:
        text.encode('ascii')
        return True
    except UnicodeEncodeError:
        return False

def detect_script_type(text):
    """
    텍스트의 문자 체계 감지
    """
    if not text:
        return None
    
    # 한글 감지
    if re.search(r'[\uac00-\ud7af]', text):
        return 'korean'
    # 일본어 감지
    if re.search(r'[\u3040-\u309f\u30a0-\u30ff]', text):
        return 'japanese'
    # 중국어 감지
    if re.search(r'[\u4e00-\u9fff]', text):
        return 'chinese'
    # 태국어 감지
    if re.search(r'[\u0e00-\u0e7f]', text):
        return 'thai'
    # 베트남어 감지
    if re.search(r'[\u1ea0-\u1ef9]', text):
        return 'vietnamese'
    
    return 'ascii' if is_ascii_only(text) else 'other'

def get_channel_id_from_handle_api(handle, api_key):
    """
    영문 핸들을 forHandle API로 조회
    """
    if not api_key:
        logger.warning("⚠️ API 키가 없어 forHandle 조회 불가")
        return None
    
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        request = youtube.channels().list(
            part='id',
            forHandle=handle.lstrip('@'),
            maxResults=1
        )
        response = request.execute()
        
        if response.get('items'):
            channel_id = response['items'][0]['id']
            logger.info(f"   ✓ forHandle API: {handle} → {channel_id}")
            return channel_id
        
        logger.warning(f"   ✗ forHandle API: {handle} 찾을 수 없음")
        return None
    
    except HttpError as e:
        logger.warning(f"   ✗ forHandle API 오류: {e.resp.status} - {e}")
        return None
    except Exception as e:
        logger.warning(f"   ✗ forHandle API 예외: {e}")
        return None

def get_channel_id_from_handle_search(handle, api_key):
    """
    비영문 핸들을 Search API로 조회
    """
    if not api_key:
        logger.warning("⚠️ API 키가 없어 Search 조회 불가")
        return None
    
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        request = youtube.search().list(
            part='snippet',
            q=handle.lstrip('@'),
            type='channel',
            maxResults=5,
            order='relevance'
        )
        response = request.execute()
        
        if response.get('items'):
            channel_id = response['items'][0]['id']['channelId']
            logger.info(f"   ✓ Search API: {handle} → {channel_id}")
            return channel_id
        
        logger.warning(f"   ✗ Search API: {handle} 찾을 수 없음")
        return None
    
    except HttpError as e:
        logger.warning(f"   ✗ Search API 오류: {e.resp.status} - {e}")
        return None
    except Exception as e:
        logger.warning(f"   ✗ Search API 예외: {e}")
        return None

def get_channel_id_from_handle_web(handle):
    """
    웹 스크래핑으로 채널ID 조회 (마지막 수단)
    실제로는 매우 제한적이므로 로그만 출력
    """
    logger.warning(f"   ⚠️ 웹 스크래핑 시도 (추천하지 않음): {handle}")
    return None

def extract_channel_id(url, handle, api_key):
    """
    URL/핸들에서 channel_id 추출 (우선순위 순서)
    1. URL에서 직접 추출
    2. 영문 핸들 → forHandle API
    3. 비영문 핸들 → Search API
    4. 웹 스크래핑 (마지막 수단)
    """
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
        logger.warning("   ✗ URL과 핸들 모두 없음")
        return None
    
    # 2. 핸들의 문자 체계 감지
    script_type = detect_script_type(handle)
    logger.info(f"   ℹ️ 핸들 타입: {handle} ({script_type})")
    
    # 3. 영문 핸들 → forHandle API
    if script_type == 'ascii':
        channel_id = get_channel_id_from_handle_api(handle, api_key)
        if channel_id:
            return channel_id
    
    # 4. 비영문 핸들 → Search API
    channel_id = get_channel_id_from_handle_search(handle, api_key)
    if channel_id:
        return channel_id
    
    # 5. 웹 스크래핑 (마지막 수단)
    channel_id = get_channel_id_from_handle_web(handle)
    if channel_id:
        return channel_id
    
    logger.error(f"   ✗ 채널ID 추출 실패: {handle}")
    return None

# ============================================================================
# 4. 범위 파싱
# ============================================================================

def parse_range(range_str, total_rows):
    """
    RANGE 환경변수 파싱
    예: "1,101" → (1, 101)
        "10-20" → (10, 20)
        None → (2, total_rows)
    """
    if not range_str:
        return (2, total_rows)  # 헤더 제외, 2부터 끝까지
    
    try:
        if ',' in range_str:
            start, end = range_str.split(',')
            return (int(start.strip()), int(end.strip()))
        elif '-' in range_str:
            start, end = range_str.split('-')
            return (int(start.strip()), int(end.strip()))
        else:
            return (2, int(range_str.strip()))
    except:
        logger.warning(f"⚠️ RANGE 파싱 실패: {range_str}, 전체 범위 사용")
        return (2, total_rows)

# ============================================================================
# 5. Step 1 메인 프로세스
# ============================================================================

def process_step1():
    """
    Step 1: YouTube 채널ID 추출
    """
    logger.info("=" * 80)
    logger.info("🚀 Step 1: YouTube 채널ID 추출 시작")
    logger.info("=" * 80)
    
    try:
        # [1/6] Google Sheets 연결
        logger.info("\n[1/6] Google Sheets 연결 중...")
        spreadsheet, worksheet_data, worksheet_api = init_google_sheets()
        
        # [2/6] 데이터 로드
        logger.info("\n[2/6] 데이터 로드 중...")
        all_values = worksheet_data.get_all_values()
        logger.info(f"✅ 총 {len(all_values)} 행 로드 완료")
        
        # [3/6] 처리 범위 결정
        logger.info("\n[3/6] 처리 범위 결정...")
        range_str = os.getenv('RANGE')
        total_rows = len(all_values)
        start_row, end_row = parse_range(range_str, total_rows)
        start_row = max(start_row, 2)  # 헤더(1행) 제외
        end_row = min(end_row, total_rows)
        logger.info(f"✅ 범위: {start_row}~{end_row}행 (총 {end_row - start_row + 1}행)")
        
        # [4/6] API 키 로드
        logger.info("\n[4/6] API 키 로드 중...")
        api_keys = load_api_keys_from_sheet(worksheet_api)
        api_key = get_first_available_api_key(api_keys)
        if api_key:
            logger.info("✅ API 키 로드 성공")
        else:
            logger.warning("⚠️ 활성화된 API 키 없음 (웹 스크래핑으로 대체)")
        
        # [5/6] 채널ID 추출
        logger.info("\n[5/6] 채널ID 추출 중...")
        channel_ids_data = []
        skipped_count = 0
        extracted_count = 0
        failed_count = 0
        
        for row_num in range(start_row, end_row + 1):
            row_idx = row_num - 1  # 0-based 인덱스
            row_data = all_values[row_idx]
            
            # 각 열 값 추출 (빈 열 처리)
            channel_name = row_data[COL_CHANNEL_NAME] if len(row_data) > COL_CHANNEL_NAME else ""
            url = row_data[COL_URL] if len(row_data) > COL_URL else ""
            handle = row_data[COL_HANDLE] if len(row_data) > COL_HANDLE else ""
            existing_channel_id = row_data[COL_CHANNEL_ID] if len(row_data) > COL_CHANNEL_ID else ""
            
            # 기존 channel_id가 있으면 스킵
            if existing_channel_id and existing_channel_id.startswith('UC'):
                logger.info(f"Row {row_num}: ⏭️ 기존 channel_id 있음: {existing_channel_id}")
                skipped_count += 1
                continue
            
            logger.info(f"\nRow {row_num}: {channel_name}")
            logger.info(f"   URL: {url}, 핸들: {handle}")
            
            # 채널ID 추출
            channel_id = extract_channel_id(url, handle, api_key)
            
            if channel_id and channel_id.startswith('UC'):
                logger.info(f"   ✅ 채널ID 추출 성공: {channel_id}")
                channel_ids_data.append({
                    'row': row_num,
                    'channel_name': channel_name,
                    'url': url,
                    'handle': handle,
                    'channel_id': channel_id
                })
                extracted_count += 1
            else:
                logger.error(f"   ❌ 채널ID 추출 실패")
                failed_count += 1
            
            # API 레이트 리미트 대비 대기
            time.sleep(0.3)
        
        # [6/6] 결과 저장
        logger.info("\n[6/6] 결과 저장 중...")
        data_dir = get_data_dir()
        os.makedirs(data_dir, exist_ok=True)
        
        output_file = os.path.join(data_dir, CHANNEL_IDS_FILE.split('/')[-1])
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(channel_ids_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"✅ 결과 저장 완료: {output_file}")
        
        # 요약
        logger.info("\n" + "=" * 80)
        logger.info("📊 Step 1 완료 요약")
        logger.info("=" * 80)
        logger.info(f"✅ 추출 성공: {extracted_count}개")
        logger.info(f"⏭️ 스킵 (기존 ID): {skipped_count}개")
        logger.info(f"❌ 추출 실패: {failed_count}개")
        logger.info(f"📁 저장 파일: {output_file}")
        logger.info(f"⏰ 완료 시간: {get_now_utc()}")
        logger.info("=" * 80)
    
    except Exception as e:
        logger.error(f"\n❌ Step 1 실패: {e}", exc_info=True)
        sys.exit(1)

# ============================================================================
# 메인 실행
# ============================================================================

if __name__ == '__main__':
    process_step1()
