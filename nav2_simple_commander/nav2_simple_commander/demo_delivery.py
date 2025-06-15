import socket
from geometry_msgs.msg import PoseStamped
from nav2_simple_commander.robot_navigator import BasicNavigator, TaskResult
import rclpy
import time
from rclpy.duration import Duration

# Destination of Map for delivery
Destination = {
    'a1': [-3.37, -0.43, '1공학관'],
    'a2': [57.87, -80.00, '국제관'],
    'a3': [66.32, 0.50, '인문경영관'],
    'b1': [-16.18, 107.70, '등나무벤치'],
    'b2': [4.00, 3.15, '학생식당'],
    'b3': [66.50, 77.50, '학생본부앞'],
    'c1': [1.84, 0.15, '해울관'],
    'c2': [50.69, 3.13, '예지관'],
    'c3': [51.78, -46.52, '예솔관'],
    'c4': [97.75, -1.96, '함지관'],
    'c5': [96.39, 34.77, '한울관']
}

# 서버 설정
HOST_IP = '0.0.0.0'
SEND_ACTIVITY_PORT = 12345         # 안드로이드 SendActivity와 통신할 포트
DELIVERY_STATUS_PORT = 12346     # 안드로이드 DeliveryStatusActivity가 리스닝할 포트
BUFFER_SIZE = 1024
server_socket_for_send_activity = None # SendActivity 용 서버 소켓
client_socket_to_send_activity = None  # SendActivity 와의 실제 통신 소켓
client_address_of_send_activity = None # SendActivity 의 주소 (IP, 포트)


def connect_to_send_activity():
    global client_socket_to_send_activity, client_address_of_send_activity, server_socket_for_send_activity
    if server_socket_for_send_activity is None:
        print("SendActivity용 서버 소켓이 초기화되지 않았습니다.")
        return False
    print(f"\n(SendActivity) 클라이언트 연결 대기 중 (포트: {SEND_ACTIVITY_PORT})...")
    try:
        client_socket_to_send_activity, client_address_of_send_activity = server_socket_for_send_activity.accept()
        print(f"[새 연결] SendActivity {client_address_of_send_activity} 에서 연결되었습니다.")
        return True
    except Exception as e:
        print(f"SendActivity 연결 수락 중 오류: {e}")
        client_socket_to_send_activity = None # 오류 시 None으로 설정
        client_address_of_send_activity = None
        return False

def receive_from_send_activity():
    global client_socket_to_send_activity # global로 사용 명시
    exit_signals = ["a1", "a2", "a3", "b1", "b2", "b3", "c1", "c2", "c3", "c4", "c5"]
    if client_socket_to_send_activity is None:
        print("SendActivity와 연결되지 않았습니다. 먼저 연결을 시도합니다.")
        if not connect_to_send_activity(): # 연결 재시도
             return False # 연결 실패 시 False 반환
    try:
        while True:
            data = client_socket_to_send_activity.recv(BUFFER_SIZE)
            if not data:
                print(f"[연결 종료] SendActivity {client_address_of_send_activity} 와의 연결이 끊어졌습니다.")
                client_socket_to_send_activity.close()
                client_socket_to_send_activity = None
                if not connect_to_send_activity(): # 다시 연결 시도
                    return False # 연결 실패 시 False 반환
                continue # 새 연결 후 다시 데이터 수신 시도

            received_str = data.decode('utf-8').strip()
            print(f"[{client_address_of_send_activity}] SendActivity로부터 수신된 데이터: '{received_str}'")
            if str(received_str) in exit_signals:
                print(f">>> 목적지 신호 '{received_str}' 수신!")
                return received_str
            else:
                print(f">>> 알 수 없는 신호 수신: '{received_str}'")
    except ConnectionResetError:
        print(f"[연결 리셋] SendActivity {client_address_of_send_activity} 와의 연결이 갑자기 끊어졌습니다.")
        if client_socket_to_send_activity: client_socket_to_send_activity.close()
        client_socket_to_send_activity = None
        return False # 연결 문제 시 False 반환
    except socket.timeout:
        print(f"[타임아웃] SendActivity {client_address_of_send_activity} 로부터 데이터 수신 중 타임아웃 발생.")
        return False
    except Exception as e:
        print(f"[에러 발생] SendActivity {client_address_of_send_activity} 처리 중 에러: {e}")
        if client_socket_to_send_activity: client_socket_to_send_activity.close()
        client_socket_to_send_activity = None
        return False


# "sc" 메시지를 DeliveryStatusActivity로 보내기 위한 새 함수
def send_sc_to_status_activity(android_ip, android_status_port, message):
    try:
        status_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        status_socket.settimeout(10) # 연결 타임아웃을 좀 더 길게 설정 (10초)
        print(f"안드로이드 DeliveryStatusActivity ({android_ip}:{android_status_port})로 '{message}' 전송 시도...")
        status_socket.connect((android_ip, android_status_port))
        status_socket.sendall(message.encode('utf-8'))
        print(f"'{message}' 메시지 전송 성공 to {android_ip}:{android_status_port}")
        status_socket.close()
        return True
    except socket.timeout:
        print(f"[타임아웃] DeliveryStatusActivity ({android_ip}:{android_status_port}) 연결 시간 초과. 앱이 해당 포트에서 리스닝 중인지 확인하세요.")
        return False
    except ConnectionRefusedError:
        print(f"[연결 거부] DeliveryStatusActivity ({android_ip}:{android_status_port})에서 연결을 거부했습니다. 앱이 해당 포트에서 리스닝 중인지 확인하세요.")
        return False
    except Exception as e:
        print(f"[메시지 전송 오류 to DeliveryStatusActivity] {e}")
        return False


def open_server_for_send_activity():
    global server_socket_for_send_activity
    server_socket_for_send_activity = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket_for_send_activity.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    print("파이썬 TCP/IP 서버 (SendActivity용) 시작합니다...")
    try:
        server_socket_for_send_activity.bind((HOST_IP, SEND_ACTIVITY_PORT))
        print(f"서버 소켓 바인딩 완료. IP: {HOST_IP}, Port: {SEND_ACTIVITY_PORT}")
        server_socket_for_send_activity.listen(1)
        print(f"SendActivity 클라이언트 연결 대기 중... (포트: {SEND_ACTIVITY_PORT})")
    except Exception as e:
        print(f"SendActivity용 서버 소켓 설정 중 오류: {e}")
        if server_socket_for_send_activity: server_socket_for_send_activity.close()
        server_socket_for_send_activity = None # 오류 시 None으로


def close_all_sockets():
    print("모든 소켓을 닫습니다...")
    global client_socket_to_send_activity, server_socket_for_send_activity
    if client_socket_to_send_activity:
        try:
            client_socket_to_send_activity.close()
            print("SendActivity 클라이언트 소켓 닫힘.")
        except Exception as e:
            print(f"SendActivity 클라이언트 소켓 닫기 오류: {e}")
        client_socket_to_send_activity = None
    if server_socket_for_send_activity:
        try:
            server_socket_for_send_activity.close()
            print("SendActivity 서버 소켓 닫힘.")
        except Exception as e:
            print(f"SendActivity 서버 소켓 닫기 오류: {e}")
        server_socket_for_send_activity = None


def main():
    rclpy.init()
    navigator = BasicNavigator()
    # ... (initial_pose 설정 및 waitUntilNav2Active 생략)

    open_server_for_send_activity()
    if not connect_to_send_activity(): # 최초 연결 시도
        close_all_sockets()
        exit(-1) # 연결 실패 시 종료

    app_destination_signal = False # 수신된 목적지 신호

    while rclpy.ok():
        if client_socket_to_send_activity is None: # SendActivity와의 연결이 끊어졌으면
            print("SendActivity와의 연결이 끊어졌습니다. 재연결 시도...")
            if not connect_to_send_activity():
                print("SendActivity 재연결 실패. 10초 후 재시도...")
                rclpy.spin_once(navigator, timeout_sec=10.0) # ROS2 노드 유지하며 대기
                continue # 루프 처음으로 돌아가 다시 연결 시도
            else:
                print("SendActivity 재연결 성공.")

        app_destination_signal = receive_from_send_activity() # SendActivity로부터 목적지 신호 수신

        if not app_destination_signal: # False 또는 None (연결 문제 등)
            print("유효한 목적지 신호를 받지 못했습니다. 다음 연결/신호 대기...")
            # client_socket_to_send_activity가 None이 되었을 수 있으므로 루프 시작 시 체크
            continue

        print(f"수신된 목적지 신호: {app_destination_signal}")

        # Delivery!!!
        shelf_item_pose = PoseStamped()
        shelf_item_pose.header.frame_id = 'map'
        shelf_item_pose.header.stamp = navigator.get_clock().now().to_msg()
        shelf_item_pose.pose.position.x = Destination[app_destination_signal][0]
        shelf_item_pose.pose.position.y = Destination[app_destination_signal][1]
        shelf_item_pose.pose.orientation.z = 1.0 # 예시 값
        shelf_item_pose.pose.orientation.w = 0.0 # 예시 값
        print(f'배달 요청 접수: {Destination[app_destination_signal][2]}.')
        navigator.goToPose(shelf_item_pose)

        i = 0
        while not navigator.isTaskComplete():
            # 이동 중에는 "sc"를 보내지 않습니다. (주석 처리 또는 삭제)
            i += 1
            feedback = navigator.getFeedback()
            if feedback and i % 5 == 0:
                print('Estimated time of arrival at ' + Destination[app_destination_signal][2] +
                      ' for worker: ' + '{0:.0f}'.format(
                          Duration.from_msg(feedback.estimated_time_remaining).nanoseconds / 1e9)
                      + ' seconds.')
                i = 0 # i 초기화는 유지

        result = navigator.getResult()
        if result == TaskResult.SUCCEEDED:
            print('제품이 정상적으로 ' + Destination[app_destination_signal][2] +'에 도착했습니다!...')
            if client_address_of_send_activity: # SendActivity의 주소 정보가 있다면
                android_device_ip = client_address_of_send_activity[0]
                print(f"최종 배달 완료. 안드로이드({android_device_ip}:{DELIVERY_STATUS_PORT})로 'sc' 전송 시도...")
                sc_sent = send_sc_to_status_activity(android_device_ip, DELIVERY_STATUS_PORT, 'sc')
                if sc_sent:
                    print("'sc' 메시지 성공적으로 전송됨.")
                else:
                    print("'sc' 메시지 전송 실패. 안드로이드 앱의 DeliveryStatusActivity가 대기 중인지 확인하세요.")
            else:
                print("클라이언트 (SendActivity) 주소 정보가 없어 'sc'를 전송할 수 없습니다.")

        elif result == TaskResult.CANCELED:
            print(f'Task at {Destination[app_destination_signal][2]} was canceled.')
            # 필요시 초기 위치로 돌아가는 로직 추가
        elif result == TaskResult.FAILED:
            print(f'Task at {Destination[app_destination_signal][2]} failed!')
            # 필요시 오류 처리 로직 추가

        # 한 번의 배달 사이클이 끝나면 SendActivity와의 연결을 유지할지,
        # 아니면 닫고 새로 받을지는 설계에 따라 결정합니다.
        # 현재는 루프를 돌며 계속 새로운 명령을 받도록 되어 있습니다.
        # 만약 SendActivity가 DeliveryStatusActivity로 전환 후 finish()된다면,
        # 다음 배달 명령을 받기 위해 client_socket_to_send_activity는 None이 되고, 루프 시작 시 재연결 시도.

    close_all_sockets()
    rclpy.shutdown() # ROS2 종료
    exit(0)

if __name__ == '__main__':
    main()
