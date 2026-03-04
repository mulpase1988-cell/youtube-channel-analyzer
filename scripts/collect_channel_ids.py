# ========================================
# YouTube 채널 ID 수집기 - GitHub Actions 버전
# X열이 비어있는 행만 처리
# ========================================

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import tempfile
import requests
import re
import time
import sys
import logging

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

# 컬럼 매핑
COL_HANDLE = 3          # C: 채널 핸들/URL
COL_CHANNEL_ID = 24     # X: channel_id

# 타임아웃 설정
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '15'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))

# ========================================
# 2. 범위 입력
# ========================================

def get_range_from_input():
    """처리 범위 가져오기"""
    start_row = int(os.environ.get('START_ROW', '2'))
    end_row_str = os.environ.get('END_ROW', '')
    end_row = int(end_row_str) if end_row_str else None
    
    return start_row, end_row

# ========================================
# 3. 채널 ID 추출 (강화된 버전)
# ========================================

def extract_channel_id(handle_or_url):
    """채널 ID 추출"""
    if not handle_or_url:
        logger.warning(f"    입력값 없음")
        return None
    
    url = str(handle_or_url).strip()
    logger.info(f"    입력: '{url}'")
    
    # 방법 1: URL에서 직접 추출
    logger.info(f"    [방법1] /channel/ 패턴 검색...")
    if '/channel/UC' in url:
        try:
            channel_id = url.split('/channel/')[-1].split('/')[0].split('?')[0]
            if channel_id.startswith('UC') and len(channel_id) == 24:
                logger.info(f"    ✅ 성공: {channel_id}")
                return channel_id
        except Exception as e:
            logger.warning(f"    파싱 오류: {e}")
    
    # 방법 2: @핸들 형식에서 채널 URL로 변환
    logger.info(f"    [방법2] @핸들 형식 검사...")
    if handle_or_url.startswith('@'):
        handle = handle_or_url[1:]  # @ 제거
        logger.info(f"    핸들명: {handle}")
        url = f"https://www.youtube.com/@{handle}"
    elif not url.startswith('http'):
        url = f"https://www.youtube.com/{url}"
    
    # 방법 3: 웹 스크래핑으로 채널 ID 추출
    logger.info(f"    [방법3] 웹 스크래핑...")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'ko-KR,ko;q=0.9',
            'Cookie': 'CONSENT=YES+KR.ko'
        }
        
        clean_url = url.split('/shorts')[0].split('/videos')[0].split('?')[0]
        logger.info(f"    요청 URL: {clean_url}")
        
        response = requests.get(
            clean_url,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )
        
        if response.status_code != 200:
            logger.warning(f"    상태 코드: {response.status_code}")
            return None
        
        logger.info(f"    상태 코드: {response.status_code}")
        
        # 여러 패턴으로 검색
        patterns = [
            r'"channelId":"(UC[a-zA-Z0-9_-]{22})"',
            r'channelId["\']?\s*:\s*["\']?(UC[a-zA-Z0-9_-]{22})',
            r'data-channel-id=["\']?(UC[a-zA-Z0-9_-]{22})',
            r'"externalChannelId":"(UC[a-zA-Z0-9_-]{22})"',
        ]
        
        for i, pattern in enumerate(patterns, 1):
            match = re.search(pattern, response.text)
            if match:
                channel_id = match.group(1)
                logger.info(f"    패턴 {i}에서 찾음: {channel_id}")
                if channel_id.startswith('UC') and len(channel_id) == 24:
                    logger.info(f"    ✅ 성공: {channel_id}")
                    return channel_id
        
        logger.warning(f"    패턴 매칭 실패")
        
    except requests.Timeout:
        logger.warning(f"    타임아웃 (제한시간: {REQUEST_TIMEOUT}초)")
    except requests.RequestException as e:
        logger.warning(f"    요청 오류: {str(e)[:50]}")
    except Exception as e:
        logger.warning(f"    오류: {str(e)[:50]}")
    
    logger.error(f"    ❌ 채널 ID 추출 실패")
    return None

# ========================================
# 4. 시트 연결
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
    
    logger.info(f"✅ '{SHEET_NAME}' - '{DATA_TAB_NAME}' 시트 연결 성공\n")
    
    return worksheet

# ========================================
# 5. 처리 대상 행 필터링
# ========================================

def get_rows_to_process(all_data, start_row, end_row):
    """X열이 비어있는 행만 추출"""
    rows_to_process = []
    
    if end_row is None:
        end_row = len(all_data)
    
    logger.info(f"📊 데이터 필터링 중...\n")
    
    for row_num in range(start_row, min(end_row + 1, len(all_data))):
        row_data = all_data[row_num - 1]
        
        # X열 값 확인
        x_value = None
        if len(row_data) >= COL_CHANNEL_ID:
            x_value = str(row_data[COL_CHANNEL_ID - 1]).strip()
        
        # C열 값 확인
        c_value = None
        if len(row_data) >= COL_HANDLE:
            c_value = str(row_data[COL_HANDLE - 1]).strip()
        
        # X열이 비어있고, C열에 값이 있으면 처리 대상
        if not x_value and c_value:
            rows_to_process.append({
                'row_num': row_num,
                'handle': c_value,
                'row_data': row_data
            })
            logger.info(f"  ✓ Row {row_num}: '{c_value}' (X열 비어있음)")
        elif x_value:
            logger.info(f"  ⏭️  Row {row_num}: X열에 이미 ID 있음 ('{x_value}')")
        elif not c_value:
            logger.info(f"  ⏭️  Row {row_num}: C열이 비어있음")
    
    logger.info(f"\n처리 대상: {len(rows_to_process)}개 행\n")
    
    return rows_to_process

# ========================================
# 6. 메인 실행
# ========================================

def main():
    """메인 실행"""
    logger.info("=" * 70)
    logger.info("🎬 YouTube 채널 ID 수집기 (X열 비어있는 행만)")
    logger.info("🔗 채널 핸들/URL에서 ID 자동 추출 및 저장")
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
        
        logger.info(f"📌 처리 범위: {start_row}행 ~ {end_row if end_row else '끝'}행\n")
        
        # 처리 대상 행 필터링
        rows_to_process = get_rows_to_process(all_data, start_row, end_row)
        
        if not rows_to_process:
            logger.info("⚠️  처리할 행이 없습니다")
            return
        
        logger.info("=" * 70)
        logger.info(f"🚀 처리 시작 ({len(rows_to_process)}개 행)")
        logger.info("=" * 70 + "\n")
        
        success_count = 0
        fail_count = 0
        batch_cells = []
        
        for idx, row_info in enumerate(rows_to_process, 1):
            row_num = row_info['row_num']
            handle = row_info['handle']
            
            logger.info(f"[{idx}/{len(rows_to_process)}] Row {row_num} 처리 중...")
            logger.info(f"  핸들: {handle}")
            
            try:
                logger.info(f"  🔍 채널 ID 추출 시도...")
                channel_id = extract_channel_id(handle)
                
                if not channel_id:
                    logger.warning(f"  ❌ 추출 실패")
                    fail_count += 1
                    time.sleep(1)
                    continue
                
                # 셀 업데이트 목록에 추가
                batch_cells.append(gspread.Cell(row_num, COL_CHANNEL_ID, channel_id))
                logger.info(f"  ✅ 완료: {channel_id}")
                success_count += 1
                
                # 배치 업데이트 (30개씩)
                if len(batch_cells) >= 30:
                    logger.info(f"\n  📤 배치 저장: {len(batch_cells)}개 셀")
                    worksheet.update_cells(batch_cells)
                    batch_cells = []
                    logger.info(f"  💤 1초 대기...\n")
                    time.sleep(1)
                
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"  ❌ 오류: {str(e)[:80]}")
                import traceback
                logger.debug(traceback.format_exc())
                fail_count += 1
                time.sleep(2)
                continue
        
        # 남은 배치 저장
        if batch_cells:
            logger.info(f"\n📤 최종 저장: {len(batch_cells)}개 셀")
            worksheet.update_cells(batch_cells)
            time.sleep(1)
        
        logger.info("\n" + "=" * 70)
        logger.info("📊 최종 결과")
        logger.info("=" * 70)
        logger.info(f"✅ 성공: {success_count}개")
        logger.info(f"❌ 실패: {fail_count}개")
        logger.info(f"📈 성공률: {success_count}/{len(rows_to_process)} ({100*success_count//len(rows_to_process)}%)")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"\n❌ 치명적 오류: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
