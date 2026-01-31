# scripts/step1_extract_channel_ids.py
"""
Step 1: YouTube 채널ID 추출 (없을 때만!)
"""

import os
import json
import re
import time
import gspread
import urllib.parse
from datetime import datetime

from config import (
    SHEET_NAME, DATA_TAB_NAME, API_KEYS_TAB_NAME,
    COL_CHANNEL_NAME, COL_URL, COL_HANDLE, COL_CHANNEL_ID,
    COL_API_KEY_NAME, COL_API_KEY_VALUE,
    API_KEY_DATA_START_ROW,
    CHANNEL_IDS_FILE, get_data_dir
)

# ============================================================================
# 1️⃣ Google Sheets 연결
# ============================================================================

def init_google_sheets():
    """Google Sheets 인증 및 연결"""
    try:
        service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT')
        
        if not service_account_json:
            raise ValueError("❌ 환경변수 'GOOGLE_SERVICE_ACCOUNT' 없음")
        
        temp_json_path = '/tmp/google_service_account.json'
        with open(temp_json_path, 'w') as f:
            f.write(service_account_json)
        
        gc = gspread.service_account(filename=temp_json_path)
        spreadsheet = gc.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(DATA_TAB_NAME)
        
        print(f"✅ Google Sheets 연결 성공")
        print(f"   스프레드시트: {SHEET_NAME}")
        print(f"   탭: {DATA_TAB_NAME}")
        
        return spreadsheet, worksheet
    
    except Exception as e:
        print(f"❌ Google Sheets 연결 실패: {e}")
        raise

# ============================================================================
# 2️⃣ Google Sheets에서 API 키 로드
# ============================================================================

def load_api_keys_from_sheet(spreadsheet):
    """Google Sheets의 'API_키_관리' 탭에서 API 키 로드"""
    try:
        api_keys_sheet = spreadsheet.worksheet(API_KEYS_TAB_NAME)
        all_values = api_keys_sheet.get_all_values()
        
        api_keys = []
        
        for idx, row in enumerate(all_values[API_KEY_DATA_START_ROW - 1:], start=API_KEY_DATA_START_ROW):
            if not row or not row[0]:
                continue
            
            key_name = row[COL_API_KEY_NAME] if COL_API_KEY_NAME < len(row) else ''
            key_value = row[COL_API_KEY_VALUE] if COL_API_KEY_VALUE < len(row) else ''
            
            if key_name and key_value:
                api_keys.append({
                    'name': key_name,
                    'key': key_value.strip(),
                    'row': idx
                })
        
        print(f"✅ API 키 로드: {len(api_keys)}개")
        for api_key in api_keys[:3]:
            key_masked = api_key['key'][:20] + '...'
            print(f"   - {api_key['name']}: {key_masked}")
        
        return api_keys
    
    except Exception as e:
        print(f"⚠️  API 키 로드 실패: {e}")
        return []

def get_first_available_api_key(api_keys):
    """사용 가능한 첫 번째 API 키"""
    if api_keys:
        return api_keys[0]['key']
    return None

# ============================================================================
# 3️⃣ URL에서 채널ID 추출
# ============================================================================

def extract_channel_id_from_url(url):
    """YouTube URL에서 channel_id 추출"""
    if not url or not isinstance(url, str):
        return None
    
    url = url.strip()
    
    match = re.search(r'/channel/(UC[a-zA-Z0-9_-]{22})', url)
    if match:
        return match.group(1)
    
    if '/@' in url:
        match = re.search(r'/@([^/?]+)', url)
        if match:
            return f"@{match.group(1)}"
    
    return None

# ============================================================================
# 4️⃣ 영문/비영문 판별
# ============================================================================

def is_ascii_only(text):
    """순수 영문인지 확인"""
    if not text:
        return False
    try:
        text.encode('ascii')
        return True
    except UnicodeEncodeError:
        return False

def detect_script_type(text):
    """문자 체계 감지"""
    if not text:
        return "UNKNOWN", "(알 수 없음)"
    
    pure_text = text.lstrip('@').strip()
    
    if any('\uac00' <= char <= '\ud7af' for char in pure_text):
        return "KOREAN", "🔤 한글"
    if any('\u4e00' <= char <= '\u9fff' for char in pure_text):
        return "CHINESE", "🔤 중국어"
    if any('\u3040' <= char <= '\u309f' or '\u30a0' <= char <= '\u30ff' for char in pure_text):
        return "JAPANESE", "🔤 일본어"
    if any('\u0600' <= char <= '\u06ff' for char in pure_text):
        return "ARABIC", "🔤 아랍어"
    if any('\u0400' <= char <= '\u04ff' for char in pure_text):
        return "RUSSIAN", "🔤 러시아어"
    if any('\u0e00' <= char <= '\u0e7f' for char in pure_text):
        return "THAI", "🔤 태국어"
    if all(char.isascii() for char in pure_text):
        return "ENGLISH", "🔤 영문"
    
    return "OTHER", "🔤 기타"

# ============================================================================
# 5️⃣ YouTube API - forHandle
# ============================================================================

def get_channel_id_from_handle_api(handle, api_key):
    """영문 핸들 → forHandle API"""
    if not handle or not api_key:
        return None
    
    try:
        from googleapiclient.discovery import build
        
        youtube = build('youtube', 'v3', developerKey=api_key)
        pure_handle = handle.lstrip('@').strip()
        
        request = youtube.channels().list(part='id', forHandle=pure_handle)
        response = request.execute()
        
        if response.get('items') and len(response['items']) > 0:
            channel_id = response['items'][0]['id']
            print(f"    ✓ forHandle API: '{pure_handle}' → {channel_id}")
            return channel_id
    
    except Exception as e:
        print(f"    ⚠️  forHandle API: {str(e)[:40]}")
    
    return None

# ============================================================================
# 6️⃣ YouTube Search API
# ============================================================================

def get_channel_id_from_handle_search(handle, api_key):
    """비영문 핸들 → Search API"""
    if not handle or not api_key:
        return None
    
    try:
        from googleapiclient.discovery import build
        
        youtube = build('youtube', 'v3', developerKey=api_key)
        pure_handle = handle.lstrip('@').strip()
        
        print(f"    🔍 Search API로 검색 중...")
        
        request = youtube.search().list(
            part='snippet',
            q=f'@{pure_handle}',
            type='channel',
            maxResults=10
        )
        response = request.execute()
        
        if response.get('items'):
            first_item = response['items'][0]
            channel_id = first_item['snippet']['channelId']
            channel_title = first_item['snippet']['title']
            
            print(f"    ✓ Search API: '{channel_title}' → {channel_id}")
            return channel_id
    
    except Exception as e:
        print(f"    ⚠️  Search API: {str(e)[:40]}")
    
    return None

# ============================================================================
# 7️⃣ 웹 스크래핑
# ============================================================================

def get_channel_id_from_handle_web(handle):
    """웹 스크래핑"""
    if not handle:
        return None
    
    try:
        import requests
        
        pure_handle = handle.lstrip('@').strip()
        encoded_handle = urllib.parse.quote(pure_handle)
        url = f'https://www.youtube.com/@{encoded_handle}'
        
        print(f"    🌐 웹 스크래핑 중...")
        
        headers = {
            'User-Agent': 'Mozilla/5.0'
        }
        
        response = requests.get(url, headers=headers, timeout=5)
        
        if response.status_code == 200:
            match = re.search(r'"externalChannelId":"(UC[a-zA-Z0-9_-]{22})"', response.text)
            if match:
                channel_id = match.group(1)
                print(f"    ✓ 웹 스크래핑: '{pure_handle}' → {channel_id}")
                return channel_id
    
    except Exception as e:
        print(f"    ⚠️  웹 스크래핑: {str(e)[:40]}")
    
    return None

# ============================================================================
# 8️⃣ 채널ID 추출
# ============================================================================

def extract_channel_id(url, handle, api_key):
    """채널ID 추출"""
    
    if url:
        channel_id = extract_channel_id_from_url(url)
        if channel_id and not channel_id.startswith('@'):
            print(f"    ✓ URL에서 추출: {channel_id}")
            return channel_id
    
    if handle and api_key:
        pure_handle = handle.lstrip('@').strip()
        
        if is_ascii_only(pure_handle):
            print(f"    🔤 영문 감지")
            channel_id = get_channel_id_from_handle_api(handle, api_key)
            if channel_id:
                return channel_id
        else:
            script_type, script_name = detect_script_type(handle)
            print(f"    {script_name} 감지")
            channel_id = get_channel_id_from_handle_search(handle, api_key)
            if channel_id:
                return channel_id
    
    if handle:
        print(f"    🔄 웹 스크래핑 시도...")
        channel_id = get_channel_id_from_handle_web(handle)
        if channel_id:
            return channel_id
    
    return None

# ============================================================================
# 9️⃣ 범위 파싱
# ============================================================================

def parse_range(range_str, total_rows):
    """범위 파싱"""
    if not range_str or not range_str.strip():
        return 2, total_rows
    
    range_str = range_str.strip()
    
    if ',' in range_str:
        parts = range_str.split(',')
        return int(parts[0].strip()), int(parts[1].strip())
    elif '-' in range_str:
        parts = range_str.split('-')
        return int(parts[0].strip()), int(parts[1].strip())
    else:
        return int(range_str), int(range_str)

# ============================================================================
# 🔟 메인 함수
# ============================================================================

def process_step1():
    """Step 1: YouTube 채널ID 추출"""
    
    print("\n" + "=" * 80)
    print("📌 Step 1: YouTube 채널ID 추출")
    print("=" * 80)
    
    # [1/6] 연결
    print("\n[1/6] Google Sheets 연결 중...")
    spreadsheet, worksheet = init_google_sheets()
    
    # [2/6] 데이터 로드
    print("\n[2/6] 데이터 로드 중...")
    try:
        all_values = worksheet.get_all_values()
        print(f"✅ {len(all_values)}개 행 로드")
    except Exception as e:
        print(f"❌ 실패: {e}")
        raise
    
    if not all_values or len(all_values) < 2:
        print("❌ 시트가 비어있습니다.")
        return
    
    # [3/6] 범위 결정
    print("\n[3/6] 범위 결정 중...")
    range_str = os.getenv('RANGE', '')
    start_row, end_row = parse_range(range_str, len(all_values))
    print(f"✅ {start_row} ~ {end_row}")
    
    # [4/6] API 키 로드
    print("\n[4/6] API 키 로드 중...")
    api_keys = load_api_keys_from_sheet(spreadsheet)
    api_key = get_first_available_api_key(api_keys)
    
    if not api_key:
        print("⚠️  API 키 없음")
    
    # [5/6] 추출
    print("\n[5/6] 채널ID 추출 중...\n")
    
    channel_ids_data = []
    existing_count = 0
    failed_count = 0
    
    for row_num in range(start_row, end_row + 1):
        if row_num >= len(all_values):
            break
        
        row_idx = row_num - 1
        row_data = all_values[row_idx]
        
        channel_name = row_data[COL_CHANNEL_NAME - 1] if COL_CHANNEL_NAME - 1 < len(row_data) else f'Row {row_num}'
        url = row_data[COL_URL - 1] if COL_URL - 1 < len(row_data) else ''
        handle = row_data[COL_HANDLE - 1] if COL_HANDLE - 1 < len(row_data) else ''
        existing_channel_id = row_data[COL_CHANNEL_ID - 1] if COL_CHANNEL_ID - 1 < len(row_data) else ''
        
        if not url and not handle:
            continue
        
        if existing_channel_id and existing_channel_id.strip().startswith('UC'):
            existing_count += 1
            print(f"✓ Row {row_num}: {channel_name} (기존 ID 있음)\n")
            continue
        
        print(f"🔍 Row {row_num}: {channel_name}")
        print(f"   URL: {url if url else '(없음)'}")
        print(f"   핸들: {handle if handle else '(없음)'}")
        
        channel_id = extract_channel_id(url, handle, api_key)
        
        if channel_id:
            print(f"   ✅ {channel_id}\n")
            channel_ids_data.append({
                'row': row_num,
                'channel_name': channel_name,
                'url': url,
                'handle': handle,
                'channel_id': channel_id
            })
        else:
            print(f"   ❌ 실패\n")
            failed_count += 1
        
        time.sleep(0.3)
    
    # [6/6] 저장
    print("[6/6] 저장 중...")
    get_data_dir()
    
    try:
        with open(CHANNEL_IDS_FILE, 'w', encoding='utf-8') as f:
            json.dump(channel_ids_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ {CHANNEL_IDS_FILE} 저장")
    except Exception as e:
        print(f"❌ 저장 실패: {e}")
        raise
    
    # 결과
    print("\n" + "=" * 80)
    print("📊 결과")
    print("=" * 80)
    print(f"기존 ID (스킵): {existing_count}")
    print(f"추출 성공: {len(channel_ids_data)}")
    print(f"추출 실패: {failed_count}")
    print(f"저장: {CHANNEL_IDS_FILE}")
    print("=" * 80)

# ============================================================================
# 시작
# ============================================================================

if __name__ == '__main__':
    try:
        process_step1()
    except Exception as e:
        print(f"\n❌ 오류: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
