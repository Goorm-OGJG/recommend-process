import json
from datetime import datetime

import redis
import numpy as np

# Redis 연결 설정
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)


def get_users_in_score_range(target_score, score_range):
    # 인바디 점수를 기반으로 범위 내의 사용자 찾기
    min_score = target_score - score_range
    max_score = target_score + score_range

    # 점수 범위에 해당하는 모든 키 조회
    keys_with_scores = redis_client.zrangebyscore('inbody_scores', min_score, max_score, withscores=True)

    # 반환할 사용자 정보 리스트
    users_in_range = []

    for key, score in keys_with_scores:
        # key 형식: "inbody:userEmail:id"
        parts = key.decode("utf-8").split(":")
        user_email = parts[1]
        routine_id = parts[2]
        inbody_id = parts[3]
        measure_at = parts[4]

        # 사용자 정보 구조화
        user_info = {
            "user_email": user_email,
            "inbody_id": inbody_id,
            "routine_id": routine_id,
            "measure_at": measure_at,
            "score": score
        }
        users_in_range.append(user_info)

    return users_in_range


def calculate_euclidean_distance(coord1, coord2):
    # 유클리디안 거리 계산
    return np.linalg.norm(coord1 - coord2)


def find_similar_inbody(user_data, users_in_score_range):
    user_id = user_data['userEmail']
    routine_id = user_data['routineId']
    inbody_id = user_data['inbodyId']
    measure_at = user_data['measureAt']

    # 사용자의 인바디 좌표 불러오기
    target_coord_key = f'inbody_coordinate:{user_id}:{routine_id}:{inbody_id}:{measure_at}'
    target_coord = json.loads(redis_client.get(target_coord_key))

    # Redis에서 모든 인바디 좌표 데이터 검색
    similar_users = []

    for user in users_in_score_range:

        other_user_id = user['user_email']
        other_routine_id = user['routine_id']
        other_inbody_id = user['inbody_id']
        other_measure_at = user['measure_at']

        key = f'inbody_coordinate:{other_user_id}:{other_routine_id}:{other_inbody_id}:{other_measure_at}'
        score_key = f'inbody:{other_user_id}:{other_routine_id}:{other_inbody_id}:{other_measure_at}'

        # 동일한 사용자의 데이터는 제외
        if key == target_coord_key:
            continue

        coord_data = json.loads(redis_client.get(key))

        distance = calculate_euclidean_distance(np.array(target_coord), np.array(coord_data))

        # 유사한 사용자 정보에 측정일 포함하여 추가
        similar_users.append({
            'user_email': other_user_id,
            'distance': distance,
            'score': redis_client.zscore('inbody_scores', score_key),
            'measure_at': other_measure_at
        })

    # 거리에 따라 정렬
    similar_users.sort(key=lambda x: x['distance'])

    return similar_users


def find_grown_users(similar_users):
    grown_users = []

    for user in similar_users:
        user_id = user['user_email']
        initial_measure_at = datetime.strptime(user['measure_at'], "%Y-%m-%d")
        initial_score = user['score']

        # 다음 측정 인바디 데이터의 키 찾기
        next_inbody_key = None
        min_diff = float('inf')

        # 해당 사용자의 모든 인바디 데이터 검색
        inbody_keys = redis_client.keys(f'inbody:{user_id}:*')

        for key in inbody_keys:
            _, user_id, routine_id, inbody_id, measure_at_date = key.decode('utf-8').split(':')
            measure_at = datetime.strptime(measure_at_date, "%Y-%m-%d")

            # 차이 계산
            diff = (measure_at - initial_measure_at).days
            if 0 < diff < min_diff:
                next_inbody_key = key.decode('utf-8')
                min_diff = diff

        # 다음 측정 인바디 데이터가 있는 경우
        if next_inbody_key:
            print(f"next_inbody_key:{next_inbody_key}")
            _, _, routine_id, _, _ = next_inbody_key.split(':')
            next_score = json.loads(redis_client.get(next_inbody_key))

            # 점수가 증가한 경우, 성장한 사용자 목록에 추가
            if next_score > initial_score:
                grown_users.append({
                    'routineId': routine_id,
                    'grownScore': next_score - initial_score
                })

    grown_users_sorted = sorted(grown_users, key=lambda x: x['grownScore'], reverse=True)

    return grown_users_sorted


def get_recommend_routines(user_data):
    # 유사한 사용자 조회
    # 	- 요청 사용자와 같은 인바디 점수 범위에 있는 사용자 조회
    # 	- 조회한 사용자들의 유클리디안 거리 계산
    users_in_score_range = get_users_in_score_range(user_data['score'], 5)
    similar_users = find_similar_inbody(user_data, users_in_score_range)
    # 사용자들의 이후 인바디의 성장 확인
    # 	- 해당 사용자가 이후 인바디 정보를 등록했고, 인바디 점수가 증가했다면 성장한 것으로 판단
    # 성장한 사용자들이 사용한 루틴을 반환
    # 	- 사용자들이 사용한 루틴을 성장률이 높은 순으로 추천
    users_with_growth = find_grown_users(similar_users)

    # log
    # print(f"similar_users: \n{similar_users}")
    # print(f"users_with_growth: \n{users_with_growth}")

    return users_with_growth
