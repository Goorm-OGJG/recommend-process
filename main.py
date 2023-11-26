import ast
import json
import traceback
from concurrent.futures import ThreadPoolExecutor

import redis
import requests

import inbody
import routine

# Redis 클라이언트 설정
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)


# 사용자 데이터 처리 및 추천 로직 실행
def process_routine_request(request):
    try:
        # 트랜잭션 시작 전에 WATCH 명령어로 키 감시
        redis_client.watch(f"inbody:{request}")

        # 트랜잭션 시작
        with redis_client.pipeline() as pipe:
            while True:
                try:
                    pipe.multi()

                    routines = routine.get_recommend_routines(request['data'])
                    redis_client.set(f"{request['uuid']}", json.dumps(routines), ex=60)

                    pipe.execute()
                    break
                except redis.WatchError:
                    # WATCH로 감시된 키가 변경되었을 경우 재시도
                    continue
    finally:
        redis_client.unwatch()


def save_user_inbody(request):
    try:
        # 트랜잭션 시작 전에 WATCH 명령어로 키 감시
        redis_client.watch(f"inbody:{request}")
        inbody_data = request['data']

        # 트랜잭션 시작
        with redis_client.pipeline() as pipe:
            while True:
                try:
                    pipe.multi()

                    try:
                        inbody.handle_user_inbody(inbody_data)
                        redis_client.set(f"{request['uuid']}", "", ex=3)
                    except Exception as e:
                        # call_inbody_delete_api(inbody_data['userEmail'], inbody_data['id'])
                        print(f"An error occurred: {e}")

                    pipe.execute()
                    break
                except redis.WatchError:
                    # WATCH로 감시된 키가 변경되었을 경우 재시도
                    continue
    finally:
        redis_client.unwatch()


def call_inbody_delete_api(user_id, inbody_id):
    url = "http://localhost:8080/api/users/inbodies"
    data = {"userId": user_id, "inbodyId": inbody_id}

    try:
        response = requests.delete(url, json=data)
        if not response.status_code == 200:
            content = response.text
            print(f"Request Error:\n{content}")
    except requests.exceptions.RequestException as e:
        print("Request failed:", e)


# 메인 함수
def process_routine_list(list_name):
    print("start redis listening..")
    while True:
        try:
            _, message = redis_client.brpop(list_name)
            if message:
                message = message.decode('utf-8')
                message = ast.literal_eval(message)

                request = json.loads(message)
                print(f"[{request['channel']}] request")

                # 채널 정보에 따른 분기 처리
                if request['channel'] == 'save_inbody':
                    save_user_inbody(request)
                elif request['channel'] == 'recommendation_routines':
                    process_routine_request(request)

        except json.JSONDecodeError:
            print("Invalid JSON format received")
            print(traceback.format_exc())
        except Exception as e:
            print(f"An error occurred: {e}")
            print(traceback.format_exc())


with ThreadPoolExecutor(max_workers=5) as executor:
    executor.submit(process_routine_list, 'routine')
