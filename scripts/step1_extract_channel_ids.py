# scripts/step1_extract_channel_ids.py
"""
Step 1: YouTube 채널ID 추출 (없을 때만!)
================================================================================

🌍 언어 지원: 한글, 일본어, 중국어, 아랍어, 러시아어, 태국어, 베트남어, 영문 등

API 키: Google Sheets의 "API_키_관리" 탭에서 자동 로드

처리 전략:
  1. URL에서 직접 추출 (가장 빠름)
  2. 영문 핸들 → forHandle API
  3. 비영문 핸들 → Search API
  4. 웹 스크래핑 (최후의 수단)
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
            raise ValueError(
                "❌ 환경변수 'GOOGLE_SERVICE_ACCOUNT' 없음\n"
                "   GitHub Secrets에서 설정하세요"
            )
        
        temp_json_path = '/tmp/google_service_account.json'
        with open(temp_json_path, 'w') as f:
            f.write(service_account_json)
        
        gc = gspread.service_account(filename=temp_json_path)
        spreadsheet = gc.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(DATA_TAB_NAME)
        
        print(f"✅ Google Sheets 연결 성공")
        print(f"   스프레드시트: {SHEET_NAME}")
        print(f"   탭: {DATA_TAB_NAME}")
        
        return spreadsheet, worksheet  # ← spreadsheet도 반환!
    
    except ValueError as e:
        print(f"❌ {e}")
        raise
    except Exception as e:
        print(f"❌ Google Sheets 연결 실패: {e}")
        raise

# ============================================================================
# 2️⃣ Google Sheets에서 API 키 로드
# ============================================================================

def load_api_keys_from_sheet(spreadsheet):
    """
    Google Sheets의 "API_키_관리" 탭에서 API 키 자동 로드
    
    반환:
      list: [
        {'name': '메인키', 'key': 'AIzaSyD_xxx', 'status': '활성', ...},
        {'name': '백업키1', 'key': 'AIzaSyD_yyy', 'status': '활성', ...},
        ...
      ]
    """
    try:
        # API 키 탭 열기
        api_keys_sheet = spreadsheet.worksheet(API_KEYS_TAB_NAME)
        all_values = api_keys_sheet.get_all_values()
        
        api_keys = []
        
        # 4행(인덱스 3)부터 데이터 읽기
        for idx, row in enumerate(all_values[API_KEY_DATA_START_ROW - 1:], start=API_KEY_DATA_START_ROW):
            if not row or not row[0]:  # 빈 행 스킵
                continue
            
            # 열 추출
            key_name = row[COL_API_KEY_NAME] if COL_API_KEY_NAME < len(row) else ''
            key_value = row[COL_API_KEY_VALUE] if COL_API_KEY_VALUE < len(row) else ''
            key_status = row[3] if 3 < len(row) else ''  # D열: 상태
            
            if key_name and key_value:
                api_keys.append({
                    'name': key_name,
                    'key': key_value.strip(),
                    'status': key_status,
                    'row': idx
                })
        
        print(f"✅ API 키 로드 성공: {len(api_keys)}개")
        for api_key in api_keys:
            key_masked = api_key['key'][:20] + '...' if len(api_key['key']) > 20 else api_key['key']
            print(f"   - {api_key['name']}: {key_masked} (상태: {api_key['status']})")
        
        return api_keys
    
    except Exception as e:
        print(f"⚠️  API 키 로드 실패: {e}")
        return []

def get_first_available_api_key(api_keys):
    """사용 가능한 첫 번째 API 키 반환"""
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
        if any('\u3040' <= char <= '\u309f' or '\u30a0' <= char <= '\u30ff' for char in pure_text):
            return "JAPANESE", "🔤 일본어"
        return "CHINESE", "🔤 중국어"
    if any('\u3040' <= char <= '\u309f' or '\u30a0' <= char <= '\u30ff' for char in pure_text):
        return "JAPANESE", "🔤 일본어"
    if any('\u0600' <= char <= '\u06ff' for char in pure_text):
        return "ARABIC", "🔤 아랍어"
    if any('\u0400' <= char <= '\u04ff' for char in pure_text):
        return "RUSSIAN", "🔤 러시아어"
    if any('\u0e00' <= char <= '\u0e7f' for char in pure_text):
        return "THAI", "🔤 태국어"
    if any('\u0100' <= char <= '\u01ff' for char in pure_text):
        return "VIETNAMESE", "🔤 베트남어"
    if all(char.isascii() for char in pure_text):
        return "ENGLISH", "🔤 영문"
    
    return "OTHER", "🔤 기타"

# ============================================================================
# 5️⃣ YouTube API - forHandle (영문)
# ============================================================================

def get_channel_id_from_handle_api(handle, api_key):
    """영문 핸들로부터 channel_id 조회 (forHandle API)"""
    if not handle or not api_key:
        return None
    
    try:
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
        
        youtube = build('youtube', 'v3', developerKey=api_key)
        pure_handle = handle.lstrip('@').strip()
        
        if not pure_handle:
            return None
        
        request = youtube.channels().list(part='id', forHandle=pure_handle)
        response = request.execute()
        
        if response.get('items') and len(response['items']) > 0:
            channel_id = response['items'][0]['id']
            print(f"    ✓ forHandle API: '{pure_handle}' → {channel_id}")
            return channel_id
        else:
            print(f"    ⚠️  forHandle API: '{pure_handle}' 조회 실패")
            return None
    
    except Exception as e:
        print(f"    ⚠️  forHandle API 오류: {str(e)[:40]}")
        return None

# ============================================================================
# 6️⃣ YouTube Search API (모든 언어)
# ============================================================================

def get_channel_id_from_handle_search(handle, api_key):
    """비영문 핸들로부터 channel_id 조회 (Search API)"""
    if not handle or not api_key:
        return None
    
    try:
        from googleapiclient.discovery import build
        
        youtube = build('youtube', 'v3', developerKey=api_key)
        pure_handle = handle.lstrip('@').strip()
        
        if not pure_handle:
            return None
        
        script_type, script_name = detect_script_type(handle)
        print(f"    {script_name} Search API로 검색 중...")
        
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
        else:
            print(f"    ⚠️  Search API: '{pure_handle}' 검색 결과 없음")
            return None
    
    except Exception as e:
        print(f"    ⚠️  Search API 오류: {str(e)[:40]}")
        return None

# ============================================================================
# 7️⃣ 웹 스크래핑 (최후)
# ============================================================================

def get_channel_id_from_handle_web(handle):
    """웹 스크래핑으로 channel_id 추출"""
    if not handle:
        return None
    
    try:
        import requests
        
        pure_handle = handle.lstrip('@').strip()
        encoded_handle = urllib.parse.quote(pure_handle)
        url = f'https://www.youtube.com/@{encoded_handle}'
        
        print(f"    🌐 웹 스크래핑 시도...")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=5)
        
        if response.status_code == 200:
            match = re.search(r'"externalChannelId":"(UC[a-zA-Z0-9_-]{22})"', response.text)
            if match:
                channel_id = match.group(1)
                print(f"    ✓ 웹 스크래핑: '{pure_handle}' → {channel_id}")
                return channel_id
        
        print(f"    ⚠️  웹 스크래핑 실패")
        return None
    
    except Exception as e:
        print(f"    ⚠️  웹 스크래핑 오류: {str(e)[:40]}")
        return None

# ============================================================================
# 8️⃣ 채널ID 추출 (모든 방식)
# ============================================================================

def extract_channel_id(url, handle, api_key):
    """채널ID 추출 (우선순위: URL → 영문 → 비영문 → 웹)"""
    
    if url:
        channel_id = extract_channel_id_from_url(url)
        if channel_id and not channel_id.startswith('@'):
            print(f"    ✓ URL에서 직접 추출: {channel_id}")
            return channel_id
    
    if handle and api_key:
        pure_handle = handle.lstrip('@').strip()
        
        if is_ascii_only(pure_handle):
            print(f"    🔤 영문 핸들 감지")
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
        print(f"    🔄 웹 스크래핑으로 시도...")
        channel_id = get_channel_id_from_handle_web(handle)
        if channel_id:
            return channel_id
    
    return None

# ============================================================================
# 9️⃣ 범위 파싱
# ============================================================================

def parse_range(range_str, total_rows):
    """RANGE 환경변수 파싱"""
    if not range_str or not range_str.strip():
        return 2, total_rows
    
    range_str = range_str.strip()
    
    if ',' in range_str:
        parts = range_str.split(',')
        start_row = int(parts[0].strip())
        end_row = int(parts[1].strip())
    elif '-' in range_str:
        parts = range_str.split('-')
        start_row = int(parts[0].strip())
        end_row = int(parts[1].strip())
    else:
        start_row = int(range_str)
        end_row = start_row
    
    return start_row, end_row

# ============================================================================
# 🔟 Step 1 메인 함수
# ============================================================================

def process_step1():
    """Step 1: YouTube 채널ID 추출 (없을 때만!)"""
    
    print("\n" + "=" * 80)
    print("📌 Step 1: YouTube 채널ID 추출 (없을 때만!) - 🌍 모든 언어 지원")
    print("=" * 80)
    
    # [1/6] Google Sheets 연결
    print("\n[1/6] Google Sheets 연결 중...")
    spreadsheet, worksheet = init_google_sheets()  # ← spreadsheet도 받기
    
    # [2/6] 모든 데이터 로드
    print("\n[2/6] Google Sheets 데이터 로드 중...")
    try:
        all_values = worksheet.get_all_values()
        print(f"✅ {len(all_values)}개 행 로드 완료")
    except Exception as e:
        print(f"❌ 데이터 로드 실패: {e}")
        raise
    
    if not all_values or len(all_values) < 2:
        print("❌ 시트가 비어있거나 헤더만 있습니다.")
        return
    
    # [3/6] 처리 범위 결정
    print("\n[3/6] 처리 범위 결정 중...")
    range_str = os.getenv('RANGE', '')
    start_row, end_row = parse_range(range_str, len(all_values))
    print(f"✅ 처리 범위: {start_row} ~ {end_row} ({end_row - start_row + 1}개 행)")
    
    # [4/6] Google Sheets에서 API 키 로드 (중요!)
    print("\n[4/6] Google Sheets에서 API 키 로드 중...")
    api_keys = load_api_keys_from_sheet(spreadsheet)
    api_key = get_first_available_api_key(api_keys)
    
    if api_key:
        print(f"✅ API 키 로드 성공 ({len(api_keys)}개 중 사용)")
    else:
        print(f"⚠️  API 키 없음 (웹 스크래핑으로 대체 가능)")
    
    # [5/6] 채널ID 추출
    print("\n[5/6] 채널ID 추출 중...\n")
    
    channel_ids_data = []
    existing_count = 0
    missing_count = 0
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
        
        # 🔑 기존 channel_id가 있으면 스킵!
        if existing_channel_id and existing_channel_id.strip().startswith('UC'):
            existing_count += 1
            print(f"✓ Row {row_num}: {channel_name}")
            print(f"  기존 channel_id 있음 → 스킵\n")
            continue
        
        # 채널ID 추출 필요
        print(f"🔍 Row {row_num}: {channel_name}")
        print(f"  📌 URL: {url[:40]}..." if len(url) > 40 else f"  📌 URL: {url if url else '(없음)'}")
        print(f"  📌 핸들: {handle if handle else '(없음)'}")
        print(f"  📌 channel_id: (비어있음) → 추출 필요")
        
        missing_count += 1
        
        channel_id = extract_channel_id(url, handle, api_key)
        
        if channel_id:
            print(f"  ✅ 추출 성공: {channel_id}\n")
            
            channel_ids_data.append({
                'row': row_num,
                'channel_name': channel_name,
                'url': url,
                'handle': handle,
                'channel_id': channel_id
            })
        else:
            print(f"  ❌ 추출 실패\n")
            failed_count += 1
        
        time.sleep(0.3)
    
    # [6/6] 결과 저장
    print("[6/6] 결과 저장 중...")
    
    get_data_dir()
    
    try:
        with open(CHANNEL_IDS_FILE, 'w', encoding='utf-8') as f:
            json.dump(channel_ids_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ JSON 파일 저장 완료: {CHANNEL_IDS_FILE}")
        
    except Exception as e:
        print(f"❌ JSON 파일 저장 실패: {e}")
        raise
    
    # 최종 결과 요약
    print("\n" + "=" * 80)
    print("📊 Step 1 완료 - 결과 요약")
    print("=" * 80)
    print(f"처리한 행: {end_row - start_row + 1}개")
    print(f"  ✓ 기존 channel_id 있음 (스킵): {existing_count}개")
    print(f"  🔍 channel_id 없음 (추출 필요): {missing_count}개")
    print(f"    ✅ 추출 성공: {len(channel_ids_data)}개")
    print(f"    ❌ 추출 실패: {failed_count}개")
    print(f"\n📁 저장된 파일: {CHANNEL_IDS_FILE}")
    print(f"📦 저장된 항목: {len(channel_ids_data)}개")
    
    if channel_ids_data:
        print(f"\n📋 추출된 채널 (샘플):")
        for i, data in enumerate(channel_ids_data[:5]):
            print(f"  [{i+1}] Row {data['row']}: {data['channel_name']}")
            print(f"      → {data['channel_id']}")
        
        if len(channel_ids_data) > 5:
            print(f"  ... 외 {len(channel_ids_data) - 5}개")
    else:
        print(f"\n✓ 모든 행에 channel_id가 이미 있습니다!")
    
    print("=" * 80)

# ============================================================================
# 1️⃣1️⃣ 진입점
# ============================================================================

if __name__ == '__main__':
    try:
        process_step1()
    except KeyboardInterrupt:
        print("\n\n⚠️  사용자가 중단함")
        exit(1)
    except Exception as e:
        print(f"\n\n❌ Step 1 실패: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
