import redis
import json
import numpy as np

# Redis 연결 설정
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)


def save_inbody_score(inbody_data):
    # 유저 ID와 인바디 ID를 결합하여 고유한 키 생성
    key = f"inbody:{inbody_data['userEmail']}:{inbody_data['routineId']}:{inbody_data['id']}:{inbody_data['measureAt']}"
    # 점수와 함께 Redis에 저장
    redis_client.zadd('inbody_scores', {key: inbody_data['score']})
    redis_client.set(key, inbody_data['score'])


def create_inbody_coordinate(inbody_data):
    # 인바디 데이터에서 좌표 생성
    return np.array([inbody_data['skeletalMuscleMass'],
                     inbody_data['bodyFatRatio'],
                     inbody_data['basalMetabolicRate'],
                     inbody_data['weight']])


def save_inbody_coordinate(user_data, inbody_coordinate):
    user_id = user_data['userEmail']
    routine_id = user_data['routineId']
    inbody_id = user_data['id']
    measure_at = user_data['measureAt']
    # Redis에 좌표 저장
    redis_client.set(f'inbody_coordinate:{user_id}:{routine_id}:{inbody_id}:{measure_at}', json.dumps(inbody_coordinate.tolist()))


def handle_user_inbody(user_inbody_data):
    save_inbody_score(user_inbody_data)
    coordinate = create_inbody_coordinate(user_inbody_data)
    save_inbody_coordinate(user_inbody_data, coordinate)
