# ========================================
# YouTube 채널 분석기 v3 - No API Key (yt-dlp 버전)
# API 키 없이 yt-dlp를 사용하여 데이터 수집
# 기능 유지: 시트 연동, 통계 계산, 썸네일 추출, 운영기간 계산
# ========================================

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta
import subprocess
import json
import time
import re
import os
import tempfile
import traceback

# ========================================
# 1. 설정 변수
# ========================================

# 🔥 환경변수에서 인증 정보 로드 (구글 시트용)
SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT')
if not SERVICE_ACCOUNT_JSON:
    # 로컬 테스트용 (환경변수가 없을 때 직접 파일 경로 지정 가능)
    # SERVICE_ACCOUNT_JSON = open('service_account.json').read() 
    raise Exception("❌ GOOGLE_SERVICE_ACCOUNT 환경변수가 설정되지 않았습니다")

# JSON을 임시 파일로 저장 (gspread 인증용)
with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
    f.write(SERVICE_ACCOUNT_JSON)
    SERVICE_ACCOUNT_FILE = f.name

# Google Sheets 설정
SHEET_NAME = os.environ.get('SHEET_NAME', '유튜브보물창고_테스트')
DATA_TAB_NAME = os.environ.get('DATA_TAB_NAME', '데이터')

# 배치 업데이트 설정
BATCH_SIZE = 20

# 컬럼 매핑 (A=1, B=2, ...) - 기존과 동일하게 유지
COL_CHANNEL_NAME = 1      # A
COL_URL = 2               # B
COL_HANDLE = 3            # C
COL_COUNTRY = 4           # D
COL_CATEGORY_1 = 5        # E
COL_CATEGORY_2 = 6        # F
COL_MEMO = 7              # G
COL_SUBSCRIBERS = 8       # H
COL_VIDEO_COUNT = 9       # I
COL_TOTAL_VIEWS = 10      # J
COL_FIRST_UPLOAD = 11     # K
COL_LATEST_UPLOAD = 12    # L
COL_COLLECT_DATE = 13     # M
COL_VIEWS_5_TOTAL = 14    # N
COL_VIEWS_10_TOTAL = 15   # O
COL_VIEWS_20_TOTAL = 16   # P
COL_VIEWS_30_TOTAL = 17   # Q
COL_KEYWORD = 18          # R
COL_NOTE = 19             # S
COL_OPERATION_DAYS = 20   # T
COL_TEMPLATE = 21         # U
COL_COUNT_5D = 22         # V
COL_COUNT_10D = 23        # W
COL_CHANNEL_ID = 24       # X
COL_VIEWS_5D = 25         # Y
COL_VIEWS_10D = 26        # Z
COL_VIEWS_15D = 27        # AA
COL_YT_CATEGORY = 28      # AB
COL_VIDEO_LINKS = [29, 30, 31, 32, 33]  # AC~AG
COL_CHANNEL_THUMBNAIL = 34  # AH

# 수동 입력 컬럼 (유지)
MANUAL_INPUT_COLUMNS = [COL_CATEGORY_1, COL_CATEGORY_2, COL_MEMO, 
                        COL_KEYWORD, COL_NOTE, COL_TEMPLATE]

# 국가 코드 매핑
COUNTRY_MAP = {
    'KR': '한국', 'US': '미국', 'JP': '일본', 'GB': '영국', 
    'DE': '독일', 'FR': '프랑스', 'CA': '캐나다', 'AU': '호주',
    'VN': '베트남', 'TH': '태국', 'ID': '인도네시아', 'IN': '인도',
    'BR': '브라질', 'MX': '멕시코', 'RU': '러시아', 'TR': '터키',
    'PH': '필리핀', 'CN': '중국', 'SG': '싱가포르', 'MY': '말레이시아'
}

# ========================================
# 2. 헬퍼 함수
# ========================================

def get_country_name(country_code):
    if not country_code: return '한국'
    return COUNTRY_MAP.get(country_code.upper(), country_code)

def run_ytdlp(url, options):
    """yt-dlp 실행 헬퍼 함수"""
    cmd = ['yt-dlp'] + options + [url]
    try:
        # 타임아웃 45초 설정 (네트워크 지연 대비)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60, encoding='utf-8')
        if result.returncode != 0:
            print(f"  ⚠️ yt-dlp 에러: {result.stderr}")
            return None
        return json.loads(result.stdout)
    except subprocess.TimeoutExpired:
        print("  ⚠️ yt-dlp 타임아웃")
        return None
    except Exception as e:
        print(f"  ⚠️ yt-dlp 실행 실패: {e}")
        return None

# ========================================
# 3. 데이터 수집 로직 (yt-dlp 사용)
# ========================================

def get_channel_data_ytdlp(channel_url, row_number, row_data):
    """yt-dlp를 사용하여 채널 데이터 수집 (API 키 불필요)"""
    
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

    print(f"  📡 yt-dlp로 정보 수집 중... (시간이 조금 걸립니다)")

    # 1단계: 채널 기본 정보 및 최신 영상 30개 가져오기
    # --dump-json: JSON 출력
    # --playlist-end 30: 최근 30개만
    # --flat-playlist: 영상 상세 정보 대신 리스트만 빠르게 (상세 정보 필요시 제거해야 함)
    # 여기서는 조회수/날짜 계산을 위해 상세 정보가 필요하므로 flat-playlist 사용 안 함
    
    # 채널 URL 정제 (shorts/videos 등의 경로 제거)
    clean_url = channel_url.split('/shorts')[0].split('/videos')[0].split('/streams')[0]
    
    # 옵션: 최신 30개 비디오의 전체 메타데이터 추출
    # ignore-errors: 일부 영상 오류 무시
    options = [
        '--dump-json', 
        '--playlist-end', '30', 
        '--ignore-errors',
        '--no-warnings'
    ]
    
    data = run_ytdlp(clean_url, options)

    if not data:
        print("  ❌ 데이터 수집 실패")
        return None

    # yt-dlp는 플레이리스트(채널)일 경우 엔트리별로 JSON이 줄바꿈되어 출력될 수 있음
    # 혹은 단일 JSON 객체 안에 'entries'가 있을 수 있음
    
    entries = []
    channel_info = {}

    # 결과 데이터 파싱 구조 처리
    if isinstance(data, dict) and 'entries' in data:
        # 단일 JSON 객체로 반환된 경우
        channel_info = data
        entries = list(data.get('entries', []))
    else:
        # 줄바꿈된 JSON 문자열들을 리스트로 처리해야 하는 경우 (run_ytdlp 수정 필요할 수 있음)
        # 위 run_ytdlp는 json.loads를 하므로 단일 객체를 기대함.
        # 만약 yt-dlp가 여러 줄의 JSON을 뱉는다면 아래 로직 필요.
        # 현재 옵션으로는 보통 첫 번째 줄에 채널 정보가 포함된 구조가 나오지 않을 수 있으므로
        # 채널 메타데이터만 별도로 가볍게 한번 더 호출하는 것이 안전함.
        pass

    # =========================================================
    # 안전성을 위해 2번 호출 전략 사용
    # 1. 채널 메타데이터 (빠름)
    # 2. 비디오 리스트 (약간 느림)
    # =========================================================
    
    # 1. 채널 메타데이터 가져오기
    print("    ↳ 채널 기본 정보 조회 중...")
    meta_opts = ['--dump-json', '--playlist-items', '0']
    meta_data = run_ytdlp(clean_url, meta_opts)
    
    if meta_data:
        result['channel_name'] = meta_data.get('uploader', '') or meta_data.get('channel', '')
        result['channel_id'] = meta_data.get('channel_id', '')
        result['subscribers'] = meta_data.get('subscriber_count', 0) or meta_data.get('channel_follower_count', 0)
        result['video_count'] = meta_data.get('video_count', 0)  # 정확하지 않을 수 있음
        result['total_views'] = meta_data.get('view_count', 0)   # 채널 전체 조회수 (일부 제공 안될 수 있음)
        
        # 썸네일 찾기 (고화질 우선)
        thumbnails = meta_data.get('thumbnails', [])
        if thumbnails:
            # 해상도 높은 순 정렬
            thumbnails.sort(key=lambda x: (x.get('height') or 0), reverse=True)
            result['channel_thumbnail'] = thumbnails[0].get('url', '')

        # 설명 등에서 핸들 추출 시도
        result['handle'] = meta_data.get('uploader_id', '') # 보통 핸들이 들어옴
        if result['handle'] and not result['handle'].startswith('@'):
            result['handle'] = '@' + result['handle']

    # 2. 최근 영상 30개 데이터 가져오기
    print(f"    ↳ 최근 영상 30개 분석 중... (채널: {result['channel_name']})")
    
    # 비디오 목록을 가져오기 위해 다시 호출 (이번엔 entries 포함)
    # --flat-playlist를 쓰면 빠르지만 duration, view_count가 정확하지 않을 때가 있음
    # 정확도를 위해 일반 모드로 30개 제한
    video_opts = ['--dump-json', '--playlist-end', '30', '--ignore-errors', '--no-warnings']
    video_data_raw = subprocess.run(['yt-dlp'] + video_opts + [clean_url], capture_output=True, text=True, encoding='utf-8')
    
    video_entries = []
    
    # yt-dlp 아웃풋이 여러 줄의 JSON일 경우 처리
    for line in video_data_raw.stdout.strip().split('\n'):
        if line.strip():
            try:
                v = json.loads(line)
                video_entries.append(v)
            except:
                continue

    # 데이터 가공
    valid_videos = []
    
    for v in video_entries:
        # 채널 정보가 1단계에서 누락되었다면 여기서 보완
        if not result['channel_name']: result['channel_name'] = v.get('uploader', '')
        if not result['channel_id']: result['channel_id'] = v.get('channel_id', '')
        
        # 영상 데이터 추출
        view_count = v.get('view_count', 0)
        upload_date_str = v.get('upload_date') # YYYYMMDD
        duration = v.get('duration', 0)
        title = v.get('title', '')
        webpage_url = v.get('webpage_url', '')
        
        # 썸네일
        thumb_url = ''
        if v.get('thumbnails'):
            thumbs = sorted(v.get('thumbnails'), key=lambda x: (x.get('height') or 0), reverse=True)
            thumb_url = thumbs[0].get('url')
        
        # 날짜 파싱
        published_at = None
        if upload_date_str:
            try:
                published_at = datetime.strptime(upload_date_str, '%Y%m%d').replace(tzinfo=timezone.utc)
            except:
                pass
                
        valid_videos.append({
            'view_count': view_count,
            'published_at': published_at,
            'thumbnail': thumb_url,
            'duration': duration,
            'title': title
        })

    # 비디오 카테고리 (첫 번째 영상 기준)
    if video_entries:
        tags = video_entries[0].get('tags', [])
        categories = video_entries[0].get('categories', [])
        if categories:
            result['yt_category'] = categories[0]
    
    # 썸네일 URL 채우기 (상위 5개)
    for i in range(min(5, len(valid_videos))):
        result['video_links'][i] = valid_videos[i]['thumbnail']
    
    # 통계 계산
    views_list = [v['view_count'] for v in valid_videos]
    result['views_5'] = sum(views_list[:5])
    result['views_10'] = sum(views_list[:10])
    result['views_20'] = sum(views_list[:20])
    result['views_30'] = sum(views_list[:30])
    
    # 기간별 조회수 및 횟수 계산
    now = datetime.now(timezone.utc)
    views_5d_list = []
    views_10d_list = []
    views_15d_list = []
    
    dates = []
    
    for v in valid_videos:
        if not v['published_at']: continue
        dates.append(v['published_at'])
        
        days_ago = (now - v['published_at']).days
        
        if days_ago <= 5:
            views_5d_list.append(v['view_count'])
        if days_ago <= 10:
            views_10d_list.append(v['view_count'])
        if days_ago <= 15:
            views_15d_list.append(v['view_count'])
            
    result['views_5d'] = sum(views_5d_list)
    result['views_10d'] = sum(views_10d_list)
    result['views_15d'] = sum(views_15d_list)
    result['count_5d'] = len(views_5d_list)
    result['count_10d'] = len(views_10d_list)

    # 날짜 및 운영기간
    if dates:
        latest_date = max(dates)
        first_date = min(dates) # 수집된 30개 중 가장 오래된 것 (전체 채널 최초가 아닐 수 있음 주의)
        
        # 전체 채널 최초 업로드 날짜는 yt-dlp flat-playlist 전체 스캔 없이는 어려움
        # 따라서 수집된 범위 내 혹은 시트에 있는 기존 값 활용 로직 필요
        # 여기서는 수집된 범위 내 최신으로 업데이트
        
        result['latest_upload'] = latest_date.strftime('%Y-%m-%d')
        
        # 운영기간 계산:
        # 기존 데이터에 최초 업로드(K열)가 있다면 그것을 유지, 없다면 현재 수집된 것 중 가장 과거(K열 갱신)
        # 하지만 yt-dlp 30개 제한이므로 최초 업로드가 아닐 확률이 높음.
        # 기존 시트의 K열 값이 있으면 유지하는 로직이 build_cell_list에 필요할 수도 있음.
        # 여기서는 일단 수집된 30개 중 가장 오래된 날짜를 넣되, 
        # 로직상 기존 값이 있고 그게 더 과거라면 기존 값을 쓰는게 맞음.
        
        existing_first = str(row_data[COL_FIRST_UPLOAD-1]).strip() if len(row_data) >= COL_FIRST_UPLOAD else ''
        
        final_first_date = first_date
        if existing_first:
            try:
                exist_dt = datetime.strptime(existing_first, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                if exist_dt < first_date:
                    final_first_date = exist_dt
            except:
                pass
        
        result['first_upload'] = final_first_date.strftime('%Y-%m-%d')
        result['operation_days'] = (latest_date - final_first_date).days

    print(f"  ✅ 수집 완료: 구독자 {result['subscribers']:,} | 조회수 {result['total_views']:,}")
    return result


# ========================================
# 4. 배치 읽기 및 쓰기 함수 (기존 유지)
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
    except Exception as e:
        print(f"⚠️ 수동 컬럼 추출 실패: {e}")
        return {col: '' for col in MANUAL_INPUT_COLUMNS}

def build_cell_list(row_num, data_dict, manual_values, row_data):
    cell_list = []
    try:
        # 기존 데이터 읽기
        existing_url = row_data[COL_URL - 1] if len(row_data) >= COL_URL else ''
        existing_country = str(row_data[COL_COUNTRY - 1]).strip() if len(row_data) >= COL_COUNTRY else ''
        
        # 국가 처리
        api_country = data_dict.get('country', '')
        final_country = api_country
        if existing_country:
            if existing_country.upper() in COUNTRY_MAP:
                final_country = COUNTRY_MAP[existing_country.upper()]
            else:
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
        print(f"❌ 셀 리스트 생성 실패: {e}")
        return []

# ========================================
# 5. 메인 실행
# ========================================
def main():
    print("=" * 60)
    print("📂 YouTube 채널 분석기 v3 - yt-dlp 버전 (No API Key)")
    print("=" * 60)

    try:
        # 구글 시트 연결
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open(SHEET_NAME)
        worksheet = spreadsheet.worksheet(DATA_TAB_NAME)
        print("✅ 구글 시트 연결 완료")

        # 범위 설정
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
        success_count = 0
        fail_count = 0

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
                
                # 수동 값 보존
                manual_values = preserve_manual_columns_batch(all_data, row_num)
                
                # 데이터 수집 (yt-dlp)
                data = get_channel_data_ytdlp(url, row_num, row_data)
                
                if data:
                    cells = build_cell_list(row_num, data, manual_values, row_data)
                    batch_cells.extend(cells)
                    batch_rows_count += 1
                    success_count += 1
                else:
                    fail_count += 1

                # 배치 업데이트
                if batch_rows_count >= BATCH_SIZE or row_num == end_row:
                    if batch_cells:
                        print(f"  📤 시트 업데이트 중 ({len(batch_cells)}개 셀)...")
                        worksheet.update_cells(batch_cells)
                        print("  ✅ 저장 완료")
                        batch_cells = []
                        batch_rows_count = 0
                        time.sleep(2) # 구글 시트 API 속도 제한 고려

            except Exception as e:
                print(f"  ❌ 에러 발생: {e}")
                traceback.print_exc()
                fail_count += 1

        print("\n" + "=" * 60)
        print(f"✅ 완료! 성공: {success_count}, 실패: {fail_count}")
        print("=" * 60)

    except Exception as e:
        print(f"❌ 치명적 오류: {e}")
        traceback.print_exc()

if __name__ == '__main__':
    main()
