def update_api_key_usage(turso_url, turso_token, api_key, quota_used, has_error=False):
    """
    API 키 사용 후 해당 정보 업데이트
    - used_quota: 사용한 할당량
    - remaining_quota: 남은 할당량
    - usage_percentage: 사용률
    - last_used: 마지막 사용 시간
    - error_count: 에러 발생 시 +1
    - test_datetime: 마지막 테스트 시간
    """
    print(f"🔄 API 키 사용 정보 업데이트 중...")
    
    try:
        current_time = datetime.now().isoformat()
        
        # Step 1: 현재 API 키 정보 조회
        sql_select = """
        SELECT used_quota, total_quota 
        FROM api_key_management 
        WHERE api_key = ?
        """
        
        result = execute_turso_query(turso_url, turso_token, sql_select, [api_key])
        
        if result and 'results' in result and len(result['results']) > 0:
            rows = result['results'][0].get('rows', [])
            if rows:
                current_used = rows[0][0] or 0
                total_quota = rows[0][1] or 10000
                
                # Step 2: 새로운 할당량 계산
                new_used_quota = current_used + quota_used
                new_remaining_quota = total_quota - new_used_quota
                new_usage_percentage = (new_used_quota / total_quota * 100) if total_quota > 0 else 0
                
                # Step 3: 에러 횟수 업데이트
                error_increment = 1 if has_error else 0
                
                # Step 4: DB 업데이트
                sql_update = """
                UPDATE api_key_management 
                SET 
                    used_quota = ?,
                    remaining_quota = ?,
                    usage_percentage = ?,
                    last_used = ?,
                    error_count = error_count + ?,
                    test_datetime = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE api_key = ?
                """
                
                execute_turso_query(
                    turso_url, turso_token, sql_update,
                    [
                        new_used_quota,
                        new_remaining_quota,
                        round(new_usage_percentage, 2),
                        current_time,
                        error_increment,
                        current_time,
                        api_key
                    ]
                )
                
                print(f"✅ API 키 정보 업데이트 완료")
                print(f"   - 사용 할당량: {current_used} → {new_used_quota}")
                print(f"   - 남은 할당량: {total_quota - current_used} → {new_remaining_quota}")
                print(f"   - 사용률: {(current_used/total_quota*100):.1f}% → {new_usage_percentage:.1f}%")
                if has_error:
                    print(f"   - 에러 발생 (+1)")
    
    except Exception as e:
        print(f"❌ API 키 정보 업데이트 실패: {str(e)}")

def call_youtube_api(api_key, country_code, category_id):
    """
    YouTube API 호출
    성공 시 데이터와 사용한 할당량 반환
    실패 시 None과 에러 정보 반환
    """
    import googleapiclient.discovery
    
    try:
        youtube = googleapiclient.discovery.build(
            'youtube', 'v3', developerKey=api_key
        )
        
        # YouTube API 호출 (할당량 1 사용)
        request = youtube.videos().list(
            chart='mostPopular',
            regionCode=country_code,
            videoCategoryId=category_id,
            part='snippet,statistics,contentDetails',
            maxResults=50
        )
        
        response = request.execute()
        
        # 할당량 1 소비됨 (YouTube API v3는 기본 100 할당량, videos.list는 1 소비)
        quota_used = 1
        
        return response, quota_used, False
    
    except Exception as e:
        print(f"❌ YouTube API 호출 실패: {str(e)}")
        # 에러 발생 시 할당량 1 소비됨 (실패해도 할당량 차감)
        return None, 1, True

def collect_hot_data(turso_url, turso_token, api_keys, countries, categories):
    """
    YouTube API를 사용해서 핫데이터 수집
    각 API 키 호출 후 정보 업데이트
    """
    print("\n🎬 YouTube API에서 데이터 수집 시작")
    print(f"   총 조합 수: {len(api_keys) * len(countries) * len(categories)}")
    
    collected_data = []
    api_key_index = 0
    total_calls = 0
    total_errors = 0
    
    for country in countries:
        for category in categories:
            try:
                # API 키 순환 (할당량 부족 시 다음 키로)
                api_key_info = api_keys[api_key_index % len(api_keys)]
                current_api_key = api_key_info['key']
                
                print(f"\n🔄 수집 중... [{total_calls + 1}/{len(api_keys) * len(countries) * len(categories)}]")
                print(f"   국가: {country['name']} ({country['code']})")
                print(f"   카테고리: {category['name']} (ID: {category['id']})")
                print(f"   API 키: {api_key_info['name']}")
                
                # Step 1: YouTube API 호출
                response, quota_used, has_error = call_youtube_api(
                    current_api_key,
                    country['code'],
                    category['id']
                )
                
                # Step 2: API 키 사용 정보 업데이트
                update_api_key_usage(
                    turso_url, turso_token,
                    current_api_key,
                    quota_used,
                    has_error
                )
                
                total_calls += 1
                if has_error:
                    total_errors += 1
                
                # Step 3: 응답 처리
                if response and 'items' in response:
                    videos = response['items']
                    print(f"   ✅ {len(videos)}개 영상 수집")
                    
                    for idx, video in enumerate(videos, 1):
                        try:
                            # 영상 데이터 파싱
                            video_data = parse_video_data(
                                video, country, category
                            )
                            collected_data.append(video_data)
                        except Exception as e:
                            print(f"      ⚠️ 영상 파싱 실패: {str(e)}")
                else:
                    print(f"   ⚠️ 영상 데이터 없음")
                
                # 다음 API 키로 순환
                api_key_index += 1
            
            except Exception as e:
                print(f"❌ 수집 실패: {str(e)}")
                total_errors += 1
                continue
    
    print(f"\n📊 수집 완료")
    print(f"   - 총 호출: {total_calls}")
    print(f"   - 성공: {total_calls - total_errors}")
    print(f"   - 실패: {total_errors}")
    print(f"   - 수집된 영상: {len(collected_data)}")
    
    return collected_data

def parse_video_data(video, country, category):
    """
    YouTube API 응답에서 필요한 데이터 추출
    global_hot_data 테이블에 맞춘 형식으로 변환
    """
    snippet = video.get('snippet', {})
    statistics = video.get('statistics', {})
    content_details = video.get('contentDetails', {})
    
    # 동영상 길이 파싱 (ISO 8601 형식)
    duration_str = content_details.get('duration', 'PT0S')
    detail_type = parse_duration(duration_str)
    
    # 태그 추출 (최대 10개)
    tags = snippet.get('tags', [])
    tags_str = ','.join(tags[:10])
    
    # 데이터 구성
    video_data = {
        'collect_datetime': datetime.now().isoformat(),
        'country': country['name'],
        'category': category['name'],
        'detail_type': detail_type,
        'ranking': 0,  # 나중에 설정
        'thumbnail': snippet.get('thumbnails', {}).get('default', {}).get('url', ''),
        'video_title': snippet.get('title', ''),
        'view_count': int(statistics.get('viewCount', 0)),
        'channel_name': snippet.get('channelTitle', ''),
        'handle': '',  # 나중에 채널 정보에서 추출
        'subscriber_count': 0,  # 나중에 채널 정보에서 추출
        'tags': tags_str,
        'video_link': f"https://www.youtube.com/watch?v={video['id']}",
        'channel_id': snippet.get('channelId', ''),
        'thumbnail_url': snippet.get('thumbnails', {}).get('high', {}).get('url', '')
    }
    
    return video_data

def parse_duration(duration_str):
    """
    ISO 8601 형식의 duration을 파싱해서 영상 타입 결정
    - Shorts: 60초 이하
    - Mid-form: 120초 이하
    - Long-form: 120초 초과
    """
    import re
    
    pattern = r'PT(\d+H)?(\d+M)?(\d+S)?'
    match = re.match(pattern, duration_str)
    
    hours = int(match.group(1)[:-1]) if match.group(1) else 0
    minutes = int(match.group(2)[:-1]) if match.group(2) else 0
    seconds = int(match.group(3)[:-1]) if match.group(3) else 0
    
    total_seconds = hours * 3600 + minutes * 60 + seconds
    
    if total_seconds <= 60:
        return 'Shorts'
    elif total_seconds <= 120:
        return 'Mid-form'
    else:
        return 'Long-form'

def insert_hot_data_to_db(turso_url, turso_token, data_rows):
    """
    수집한 데이터를 global_hot_data 테이블에 삽입
    """
    print(f"\n💾 {len(data_rows)}개 영상을 DB에 삽입 중...")
    
    inserted_count = 0
    
    for row in data_rows:
        try:
            sql = """
            INSERT INTO global_hot_data 
            (collect_datetime, country, category, detail_type, ranking, 
             thumbnail, video_title, view_count, channel_name, handle, 
             subscriber_count, tags, video_link, channel_id, thumbnail_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            execute_turso_query(
                turso_url, turso_token, sql,
                [
                    row['collect_datetime'],
                    row['country'],
                    row['category'],
                    row['detail_type'],
                    row['ranking'],
                    row['thumbnail'],
                    row['video_title'],
                    row['view_count'],
                    row['channel_name'],
                    row['handle'],
                    row['subscriber_count'],
                    row['tags'],
                    row['video_link'],
                    row['channel_id'],
                    row['thumbnail_url']
                ]
            )
            inserted_count += 1
        
        except Exception as e:
            print(f"⚠️ 행 삽입 실패: {str(e)}")
    
    print(f"✅ {inserted_count}/{len(data_rows)}개 영상 삽입 완료")
    return inserted_count

def main():
    """메인 함수"""
    print("="*70)
    print("🎬 글로벌 핫데이터 수집기 시작")
    print("="*70)
    
    # ... (기존 Step 1-5 코드)
    
    # Step 7: YouTube API 데이터 수집 및 API 키 정보 업데이트
    print("\n🎯 Step 7: YouTube API 데이터 수집")
    collected_data = collect_hot_data(
        final_turso_url, final_turso_token,
        api_keys, countries, categories
    )
    
    # Step 8: 수집한 데이터를 DB에 삽입
    if collected_data:
        inserted_count = insert_hot_data_to_db(
            final_turso_url, final_turso_token,
            collected_data
        )
    
    print("\n" + "="*70)
    print("✅ 글로벌 핫데이터 수집 완료!")
    print("="*70)

if __name__ == '__main__':
    main()
