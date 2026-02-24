# ========================================
# YouTube 채널 분석기 v7 - Shorts 탭 필수 체크
# 해결: "Scrapetube 완료: 0개" 문제 해결
# 전략: Videos 탭이 비어있으면 Shorts 탭을 자동으로 탐색
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
import random

# ========================================
# 1. 설정 및 환경변수
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
COL_CHANNEL_NAME = 1
COL_URL = 2
COL_HANDLE = 3
COL_SUBSCRIBERS = 8
COL_LATEST_UPLOAD = 12
COL_COLLECT_DATE = 13
COL_CHANNEL_ID = 24
COL_VIEWS_5D = 25
COL_VIEWS_10D = 26
COL_VIEWS_15D = 27
COL_VIDEO_LINKS = [29, 30, 31, 32, 33]

# ========================================
# 2. 헬퍼 함수
# ========================================

def parse_korean_count(text):
    """조회수 텍스트 파싱"""
    if not text: return 0
    try:
        text = str(text).strip()
        text = re.sub(r'[^0-9.천만억조KMB]', '', text)
        if not text: return 0
        num_match = re.search(r'(\d+\.?\d*)', text)
        if not num_match: return 0
        num = float(num_match.group(1))
        
        if '조' in text: return int(num * 1000000000000)
        elif '억' in text: return int(num * 100000000)
        elif '만' in text or 'M' in text: return int(num * 10000) if '만' in text else int(num * 1000000)
        elif '천' in text or 'K' in text: return int(num * 1000)
        else: return int(num)
    except: return 0

def get_range_from_input():
    if len(sys.argv) >= 3:
        try: return int(sys.argv[1]), int(sys.argv[2])
        except: pass
    range_input = os.environ.get('RANGE', '').strip()
    if range_input:
        if '-' in range_input:
            try: s, e = map(int, range_input.split('-')); return s, e
            except: pass
        else:
            try: return int(range_input), None
            except: pass
    return 2, None

# ========================================
# 3. 데이터 수집 핵심 (RSS + Scrapetube Multi-Tab)
# ========================================

def get_channel_data_v7(channel_id):
    """
    RSS로 기본 정보를 확보하고, Scrapetube로 Shorts/Videos 탭을 모두 뒤져서 조회수를 확보
    """
    if not channel_id: return None

    # [Step 1] RSS로 데이터 우선 확보 (실패시에도 최소한의 데이터 보장)
    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    print(f"  📡 RSS 데이터 요청 중...")
    
    rss_videos = []
    rss_map = {} # video_id -> published_date
    
    try:
        feed = feedparser.parse(rss_url)
        for entry in feed.entries:
            vid = None
            if hasattr(entry, 'yt_videoid'): vid = entry.yt_videoid
            elif 'id' in entry: vid = entry.id.split(':')[-1]
            
            if vid:
                pub_date = 'Unknown'
                try:
                    dt = dateutil_parser.parse(entry.published)
                    if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
                    pub_date = dt.strftime('%Y-%m-%d %H:%M:%S')
                except: pass
                
                # 썸네일
                thumb = f"https://i.ytimg.com/vi/{vid}/mqdefault.jpg"
                if hasattr(entry, 'media_thumbnail'):
                    thumb = entry.media_thumbnail[0]['url']
                
                rss_map[vid] = pub_date
                rss_videos.append({
                    'video_id': vid,
                    'title': entry.title,
                    'published_date': pub_date,
                    'views': 0, # RSS는 조회수 없음
                    'thumbnail_url': thumb
                })
    except Exception as e:
        print(f"  ⚠️ RSS 에러: {e}")

    print(f"  ✅ RSS 완료: {len(rss_videos)}개 영상 확보")

    # [Step 2] Scrapetube로 조회수 채우기 (Shorts -> Videos 순서로 탐색)
    # 쇼츠 채널일 확률이 높으므로 shorts를 먼저 탐색
    content_types = ['shorts', 'videos', 'streams'] 
    
    merged_videos = {} # video_id -> video_obj
    
    # RSS 데이터를 기본으로 채움
    for v in rss_videos:
        merged_videos[v['video_id']] = v

    found_in_scrapetube = False
    
    for c_type in content_types:
        # 이미 충분한 데이터를 찾았고, RSS에 있는 영상들의 조회수를 찾았다면 중단
        if found_in_scrapetube: 
            break
            
        print(f"  👁️ 탭 검색 중: {c_type}...")
        
        try:
            # sleep_interval로 차단 방지
            scrape_gen = scrapetube.get_channel(channel_id, content_type=c_type, limit=30, sleep_interval=0.1)
            
            count = 0
            for v in scrape_gen:
                count += 1
                video_id = v.get('videoId')
                if not video_id: continue
                
                # 조회수 파싱
                view_count = 0
                if 'viewCountText' in v:
                    vt = v['viewCountText']
                    label = vt.get('simpleText', '')
                    if not label and 'runs' in vt:
                        label = vt['runs'][0].get('text', '')
                    if not label:
                        label = vt.get('accessibility', {}).get('accessibilityData', {}).get('label', '')
                    view_count = parse_korean_count(label)
                
                # 썸네일
                thumb = f"https://i.ytimg.com/vi/{video_id}/mqdefault.jpg"
                if 'thumbnail' in v and 'thumbnails' in v['thumbnail']:
                    thumb = v['thumbnail']['thumbnails'][-1]['url']
                
                # RSS에 있는 영상이면 업데이트, 없으면 추가
                if video_id in merged_videos:
                    merged_videos[video_id]['views'] = view_count
                    merged_videos[video_id]['thumbnail_url'] = thumb # 더 좋은 썸네일일 수 있음
                else:
                    # RSS엔 없지만(오래된 영상 등) Scrapetube엔 있는 경우
                    # 날짜는 알 수 없으므로 Unknown 처리하거나 RSS 맵에 없으면 무시할 수도 있음
                    # 여기서는 추가함
                    merged_videos[video_id] = {
                        'video_id': video_id,
                        'title': '', # 제목 파싱 생략
                        'published_date': 'Unknown', # Scrapetube는 정확한 날짜 안줌
                        'views': view_count,
                        'thumbnail_url': thumb
                    }
            
            if count > 0:
                print(f"    ✓ {c_type} 탭에서 {count}개 발견")
                found_in_scrapetube = True
                
        except Exception as e:
            # print(f"    ⚠️ {c_type} 탐색 중 오류: {e}")
            pass

    final_list = list(merged_videos.values())
    
    # 만약 Scrapetube가 모두 실패했더라도 RSS 데이터가 있으면 반환 (성공 처리)
    if not found_in_scrapetube and rss_videos:
        print("  ⚠️ 조회수 수집 실패했으나 RSS 데이터로 대체합니다 (조회수 0)")
        return rss_videos
        
    return final_list

# ========================================
# 4. ID 추출
# ========================================

def extract_channel_id(handle_or_url):
    if not handle_or_url: return None
    url = str(handle_or_url).strip()
    
    if '/channel/UC' in url:
        return url.split('/channel/')[-1].split('/')[0].split('?')[0]
        
    # 헤더와 쿠키를 사용하여 봇 차단 우회 시도
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cookie': 'CONSENT=YES+KR.ko+20150622-23-0'
    }
    
    try:
        clean_url = url.split('/shorts')[0].split('/videos')[0]
        if not clean_url.startswith('http'):
            clean_url = f"https://www.youtube.com/{clean_url}" if clean_url.startswith('@') else f"https://www.youtube.com/@{clean_url}"
            
        res = requests.get(clean_url, headers=headers, timeout=10)
        
        match = re.search(r'"channelId":"(UC[^"]+)"', res.text)
        if match: return match.group(1)
        
        match = re.search(r'"browseId":"(UC[^"]+)"', res.text)
        if match: return match.group(1)
        
    except: pass
    return None

# ========================================
# 5. 시트 업데이트
# ========================================

def connect_to_sheet():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    gc = gspread.authorize(creds)
    return gc.open(SHEET_NAME).worksheet(DATA_TAB_NAME)

def calculate_stats(videos):
    # 날짜순 정렬 (Unknown은 맨 뒤로)
    sorted_v = sorted(videos, key=lambda x: x['published_date'] if x['published_date'] != 'Unknown' else '0000', reverse=True)
    
    return {
        'views_5': sum(v['views'] for v in sorted_v[:5]),
        'views_10': sum(v['views'] for v in sorted_v[:10]),
        'views_15': sum(v['views'] for v in sorted_v[:15]),
        'videos_5': sorted_v[:5]
    }

def update_row(row_num, channel_id, stats):
    cells = []
    if channel_id:
        cells.append(gspread.Cell(row_num, COL_CHANNEL_ID, channel_id))
    
    if stats['videos_5']:
        latest = stats['videos_5'][0]['published_date']
        if latest != 'Unknown':
            cells.append(gspread.Cell(row_num, COL_LATEST_UPLOAD, latest.split(' ')[0]))
            
    cells.append(gspread.Cell(row_num, COL_COLLECT_DATE, datetime.now(timezone.utc).strftime('%Y-%m-%d')))
    cells.append(gspread.Cell(row_num, COL_VIEWS_5D, stats['views_5']))
    cells.append(gspread.Cell(row_num, COL_VIEWS_10D, stats['views_10']))
    cells.append(gspread.Cell(row_num, COL_VIEWS_15D, stats['views_15']))
    
    for i, col in enumerate(COL_VIDEO_LINKS):
        if i < len(stats['videos_5']):
            cells.append(gspread.Cell(row_num, col, stats['videos_5'][i]['thumbnail_url']))
            
    return cells

# ========================================
# 6. 메인 실행
# ========================================

def main():
    print("="*60)
    print("🚀 YouTube 분석기 v7 - Shorts 탭 우선 검색")
    print("="*60)
    
    try:
        worksheet = connect_to_sheet()
        all_data = worksheet.get_all_values()
        start_row, end_row = get_range_from_input()
        if end_row is None: end_row = len(all_data)
        
        print(f"📌 처리 범위: {start_row} ~ {end_row}")
        
        batch_cells = []
        success = 0
        fail = 0
        
        for row_num in range(start_row, min(end_row + 1, len(all_data) + 1)):
            print(f"\n🔍 Row {row_num} 처리 중...")
            try:
                if row_num - 1 >= len(all_data): break
                row_data = all_data[row_num - 1]
                
                cid = None
                if len(row_data) >= COL_CHANNEL_ID:
                    exist = str(row_data[COL_CHANNEL_ID-1]).strip()
                    if exist.startswith('UC'): cid = exist
                
                if not cid and len(row_data) >= COL_HANDLE:
                    raw_h = str(row_data[COL_HANDLE-1]).strip()
                    if raw_h: cid = extract_channel_id(raw_h)
                
                if not cid:
                    print("  ❌ 채널 ID 없음")
                    fail += 1
                    continue
                
                print(f"  🔗 ID: {cid}")
                videos = get_channel_data_v7(cid)
                
                if not videos:
                    print("  ❌ 데이터 수집 실패 (RSS/Scrapetube 모두 실패)")
                    fail += 1
                    continue
                
                stats = calculate_stats(videos)
                new_cells = update_row(row_num, cid, stats)
                batch_cells.extend(new_cells)
                success += 1
                
                print(f"  ✅ 수집 완료: 5개합계 {stats['views_5']:,}")
                
                if len(batch_cells) >= 20:
                    worksheet.update_cells(batch_cells)
                    print("  📤 배치 저장 완료")
                    batch_cells = []
                    time.sleep(2)
                
                time.sleep(1)
                
            except Exception as e:
                print(f"  ❌ 에러: {e}")
                fail += 1
                
        if batch_cells:
            worksheet.update_cells(batch_cells)
            print("📤 최종 저장 완료")
            
        print(f"\n✅ 성공: {success} | ❌ 실패: {fail}")
        
    except Exception as e:
        print(f"❌ 치명적 오류: {e}")

if __name__ == '__main__':
    main()
