import threading

# Define message lists and events
messages_sensor_data_response = []
messages_consumed_sensor_data_event = threading.Event()

messages_get_all_sensor_data_response = []
messages_consumed_get_all_sensor_data_event = threading.Event()

messages_get_by_id_sensor_data_response = []
messages_consumed_get_by_id_sensor_data_event = threading.Event()

messages_camera_response = []
messages_consumed_camera_event = threading.Event()

messages_get_all_camera_response = []
messages_consumed_get_all_camera_event = threading.Event()

messages_get_by_id_camera_response = []
messages_consumed_get_by_id_camera_event = threading.Event()

messages_prediction_response = []
messages_consumed_prediction_event = threading.Event()

# Define locks for each message list
lock_sensor_data_response = threading.Lock()
lock_get_all_sensor_data_response = threading.Lock()
lock_get_by_id_sensor_data_response = threading.Lock()
lock_camera_response = threading.Lock()
lock_get_all_camera_response = threading.Lock()
lock_get_by_id_camera_response = threading.Lock()
lock_prediction_response = threading.Lock()


# Pod-related variables
messages_pod_response = []
messages_get_all_pods_response = []
messages_get_by_id_pod_response = []
messages_delete_pod_response = []
messages_update_pod_response = []
messages_consumed_pod_event = threading.Event()
messages_consumed_get_all_pods_event = threading.Event()
messages_consumed_get_by_id_pod_event = threading.Event()
messages_consumed_delete_pod_event = threading.Event()
messages_consumed_update_pod_event = threading.Event()
lock_pod_response = threading.Lock()
lock_get_all_pods_response = threading.Lock()
lock_get_by_id_pod_response = threading.Lock()
lock_delete_pod_response = threading.Lock()
lock_update_pod_response = threading.Lock()

# Device-related variables
messages_device_response = []
messages_get_all_devices_response = []
messages_get_by_id_device_response = []
messages_get_devices_by_pod_response = []
messages_delete_device_response = []
messages_update_device_response = []
messages_consumed_device_event = threading.Event()
messages_consumed_get_all_devices_event = threading.Event()
messages_consumed_get_by_id_device_event = threading.Event()
messages_consumed_get_devices_by_pod_event = threading.Event()
messages_consumed_delete_device_event = threading.Event()
messages_consumed_update_device_event = threading.Event()
lock_device_response = threading.Lock()
lock_get_all_devices_response = threading.Lock()
lock_get_by_id_device_response = threading.Lock()
lock_get_devices_by_pod_response = threading.Lock()
lock_delete_device_response = threading.Lock()
lock_update_device_response = threading.Lock()

# Bucket-related variables
messages_bucket_response = []
messages_get_all_buckets_response = []
messages_get_by_id_bucket_response = []
messages_get_buckets_by_device_response = []
messages_delete_bucket_response = []
messages_update_bucket_response = []
messages_consumed_bucket_event = threading.Event()
messages_consumed_get_all_buckets_event = threading.Event()
messages_consumed_get_by_id_bucket_event = threading.Event()
messages_consumed_get_buckets_by_device_event = threading.Event()
messages_consumed_delete_bucket_event = threading.Event()
messages_consumed_update_bucket_event = threading.Event()
lock_bucket_response = threading.Lock()
lock_get_all_buckets_response = threading.Lock()
lock_get_by_id_bucket_response = threading.Lock()
lock_delete_bucket_response = threading.Lock()
lock_update_bucket_response = threading.Lock()
lock_get_buckets_by_device_response = threading.Lock()


